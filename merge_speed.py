import requests
import concurrent.futures
import re
from collections import defaultdict
import time
from urllib.parse import urlparse
import m3u8

# ========== 全局配置优化 ==========
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
TEST_TIMEOUT = 2.0       # 缩短超时，更快丢弃卡死源
MAX_BEST_PER_CHANNEL = 3
MIN_ULTRA_HEIGHT = 1080  # 超清标准1080P/4K
MIN_HD_HEIGHT = 720      # 高清标准720P，保留高速720P优质源
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

# 解析m3u8，获取最高分辨率高度，单流无标签返回720（不再判0丢弃）
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
        # 单一流无分辨率标签，默认标记为720P，保留不丢弃
        return 720
    except Exception:
        # 解析失败无法获取分辨率，仍允许进入测速，不直接淘汰
        return 720

# 双重检测：先测速，再区分超清/高清，不再一刀切删除720P
def test_link_quality(url):
    headers = {"User-Agent":"Mozilla/5.0 Android TV"}
    # 先测速，保证高速源不会直接跳过
    start = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=TEST_TIMEOUT, stream=True)
        r.raw.read(1024)
        delay = round(time.time() - start, 3)
    except Exception:
        return (9999, 0)
    # 测速成功后再获取分辨率
    height = get_max_video_height(url)
    return (delay, height)

def main():
    white_order_list = load_white_list_ordered()
    source_urls = load_source_urls()
    all_channels = []

    # 降低并发线程，避免Action卡死超长耗时
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        res_list = executor.map(fetch_iptv, source_urls)
        for res in res_list:
            all_channels.extend(res)

    # 按频道分组，移除严格域名去重，保留更多同域名优质线路
    group = defaultdict(list)
    for name,url in all_channels:
        group[name].append(url)

    final_output = []
    # 第一分组：央视频道
    final_output.append("央视频道,#genre#")
    for ch_name in white_order_list:
        if ch_name.startswith("CCTV") and ch_name in group:
            url_list = group[ch_name]
            ultra_links = []  # 1080P+超清
            hd_links = []     # 720P高清高速备选
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, height) in zip(url_list, results):
                if delay >= TEST_TIMEOUT:
                    continue
                # 分级存储，超清优先
                if height >= MIN_ULTRA_HEIGHT:
                    ultra_links.append((delay, link))
                else:
                    hd_links.append((delay, link))
            # 超清优先放前面，不足3条用高速720P补充
            ultra_links.sort(key=lambda x: x[0])
            hd_links.sort(key=lambda x: x[0])
            top_links = [item[1] for item in ultra_links + hd_links][:MAX_BEST_PER_CHANNEL]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 第二分组：卫视频道
    final_output.append("\n卫视频道,#genre#")
    kid_names = {"CCTV14","金鹰卡通","卡酷少儿","优漫卡通","嘉佳卡通","哈哈炫动","CETV早期教育","山东教育","河北少儿科教","内蒙古少儿","新疆少儿"}
    for ch_name in white_order_list:
        if not ch_name.startswith("CCTV") and ch_name not in kid_names and ch_name in group:
            url_list = group[ch_name]
            ultra_links = []
            hd_links = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, height) in zip(url_list, results):
                if delay >= TEST_TIMEOUT:
                    continue
                if height >= MIN_ULTRA_HEIGHT:
                    ultra_links.append((delay, link))
                else:
                    hd_links.append((delay, link))
            ultra_links.sort(key=lambda x: x[0])
            hd_links.sort(key=lambda x: x[0])
            top_links = [item[1] for item in ultra_links + hd_links][:MAX_BEST_PER_CHANNEL]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 第三分组：少儿动画频道
    final_output.append("\n少儿动画频道,#genre#")
    kid_list = ["CCTV14","金鹰卡通","卡酷少儿","优漫卡通","嘉佳卡通","哈哈炫动","CETV早期教育","山东教育","河北少儿科教","内蒙古少儿","新疆少儿"]
    for ch_name in kid_list:
        if ch_name in group:
            url_list = group[ch_name]
            ultra_links = []
            hd_links = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(test_link_quality, url_list)
            for link, (delay, height) in zip(url_list, results):
                if delay >= TEST_TIMEOUT:
                    continue
                if height >= MIN_ULTRA_HEIGHT:
                    ultra_links.append((delay, link))
                else:
                    hd_links.append((delay, link))
            ultra_links.sort(key=lambda x: x[0])
            hd_links.sort(key=lambda x: x[0])
            top_links = [item[1] for item in ultra_links + hd_links][:MAX_BEST_PER_CHANNEL]
            for l in top_links:
                final_output.append(f"{ch_name},{l}")

    # 写入tv.txt，超清在前、高速720P备选在后
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(final_output))
    print(f"筛选完成：超清1080P优先，补充高速720P备选，有效线路总数：{len(final_output)}")

if __name__ == "__main__":
    main()
