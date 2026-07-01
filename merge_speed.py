import requests
import concurrent.futures
import re
from collections import defaultdict
import time
from urllib.parse import urlparse
import m3u8

# ========== 全局配置（1080P起步，低于全部过滤） ==========
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
TEST_TIMEOUT = 2.5
MAX_BEST_PER_CHANNEL = 3
MIN_HEIGHT = 1080  # 仅保留1080P、2K、4K，720/480/360全部丢弃
BLACK_HOST = {"127.", "192.", "10.", "172.", "localhost", ":8801", ":808"}

# 有序读取白名单，严格保持自定义顺序
def load_white_list_ordered():
    white_channels_order = []
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            white_channels_order.append(line)
    return white_channels_order

# 读取外部直播源列表
def load_source_urls():
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        lines = [i.strip() for i in f.readlines() if i.strip()]
    return lines

# 过滤内网/局域网无法外网播放的地址
def filter_private_url(url):
    for black in BLACK_HOST:
        if black in url:
            return False
    return True

# 抓取所有txt/m3u8内频道与链接
def fetch_iptv(url):
    channels = []
    try:
        headers = {"User-Agent":"Mozilla/5.0 Android TV"}
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = resp.apparent_encoding
        text = resp.text
        # 标准txt格式匹配
        txt_pattern = re.compile(r"([^,]+),(http[^\n]+)")
        txt_matches = txt_pattern.findall(text)
        for name, link in txt_matches:
            name = name.strip().replace("#genre#","").strip()
            link = link.strip()
            if name and link and filter_private_url(link):
                channels.append((name, link))
        # m3u8格式匹配
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[^\n]+)")
        m3u_matches = m3u_pattern.findall(text)
        for name, link in m3u_matches:
            name = name.strip()
            link = link.strip()
            if name and link and filter_private_url(link):
                channels.append((name, link))
    except Exception:
        pass
    return channels

# 解析m3u8，获取最高分辨率高度
def get_max_video_height(m3u8_url):
    headers = {"User-Agent":"Mozilla/5.0 Android TV"}
    try:
        resp = requests.get(m3u8_url, headers=headers, timeout=2)
        playlist = m3u8.loads(resp.text)
        max_h = 0
        # 多码率分片，自动取最高清分辨率
        if playlist.is_variant:
            for pl in playlist.playlists:
                if hasattr(pl.stream_info, "resolution") and pl.stream_info.resolution:
                    width, height = pl.stream_info.resolution.split("x")
                    h_val = int(height)
                    if h_val > max_h:
                        max_h = h_val
            return max_h
        # 单一流无分辨率标签，直接判定不足1080P过滤
        return 0
    except Exception:
        return 0

# 双重检测：分辨率达标 + 网络测速
def test_link_quality(url):
    height = get_max_video_height(url)
    # 低于1080直接淘汰，不进行测速
    if height < MIN_HEIGHT:
        return (9999, False)
    headers = {"User-Agent":"Mozilla/5.0 Android TV"}
    start = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=TEST_TIMEOUT, stream=True)
        r.raw.read(1024)
        delay = round(time.time() - start, 3)
        return (delay, True)
    except Exception:
        return (9999, False)

def main():
    white_order_list = load_white_list_ordered()
    source_urls = load_source_urls()
    all_channels = []

    # 多线程拉取全部外部源
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        res_list = executor.map(fetch_iptv, source_urls)
        for res in res_list:
            all_channels.extend(res)

    # 按频道分组 + 同域名去重，避免同一服务器多条冗余线路
    group = defaultdict(list)
    domain_set = defaultdict(set)
    for name,url in all_channels:
        domain = urlparse(url).netloc
        if domain not in domain_set[name]:
            domain_set[name].add(domain)
            group[name].append(url)

    final_output = []
    # 第一分组：央视频道
    final_output.append("央视频道,#genre#")
    for ch_name in white_order_list:
        if ch_name.startswith("CCTV") and ch_name in group:
            url_list = group[ch_name]
            qualified = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, is_1080p_plus) in zip(url_list, results):
                if is_1080p_plus and delay < TEST_TIMEOUT:
                    qualified.append((delay, link))
            # 按延迟升序，取最快3条高清线路
            qualified.sort(key=lambda x: x[0])
            top_links = [item[1] for item in qualified[:MAX_BEST_PER_CHANNEL]]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 第二分组：卫视频道
    final_output.append("\n卫视频道,#genre#")
    kid_names = {"CCTV14","金鹰卡通","卡酷少儿","优漫卡通","嘉佳卡通","哈哈炫动","CETV早期教育","山东教育","河北少儿科教","内蒙古少儿","新疆少儿"}
    for ch_name in white_order_list:
        if not ch_name.startswith("CCTV") and ch_name not in kid_names and ch_name in group:
            url_list = group[ch_name]
            qualified = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, is_1080p_plus) in zip(url_list, results):
                if is_1080p_plus and delay < TEST_TIMEOUT:
                    qualified.append((delay, link))
            qualified.sort(key=lambda x: x[0])
            top_links = [item[1] for item in qualified[:MAX_BEST_PER_CHANNEL]]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 第三分组：少儿动画频道
    final_output.append("\n少儿动画频道,#genre#")
    kid_list = ["CCTV14","金鹰卡通","卡酷少儿","优漫卡通","嘉佳卡通","哈哈炫动","CETV早期教育","山东教育","河北少儿科教","内蒙古少儿","新疆少儿"]
    for ch_name in kid_list:
        if ch_name in group:
            url_list = group[ch_name]
            qualified = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, is_1080p_plus) in zip(url_list, results):
                if is_1080p_plus and delay < TEST_TIMEOUT:
                    qualified.append((delay, link))
            qualified.sort(key=lambda x: x[0])
            top_links = [item[1] for item in qualified[:MAX_BEST_PER_CHANNEL]]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 写入纯净1080P+直播列表
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(final_output))
    print(f"筛选完成，仅保留1080P/2K/4K高清源，有效线路总数：{len(final_output)}")

if __name__ == "__main__":
    main()
