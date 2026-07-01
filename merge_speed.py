import requests
import concurrent.futures
import re
from collections import defaultdict
import time
import m3u8

# ===================== 全局配置（无需修改） =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
TEST_TIMEOUT = 2.0
MIN_VIDEO_HEIGHT = 720    # 低于720p直接剔除
MAX_PER_CHANNEL = 3       # 单频道最多3条线路
# 过滤局域网/组播不兼容地址（三端播放器无法播放）
BLACK_HOST = {"127.", "192.168.", "10.", "172.", "localhost", ":8801", "igmp://", "rtmp://"}
# 并发线程控制，适配Github Actions低配机器
SOURCE_CHECK_WORKER = 8
CHANNEL_TEST_WORKER = 12

# ===================== 工具函数 =====================
def is_incompatible_url(url: str) -> bool:
    """过滤内网、组播、rtmp等三端不兼容链接"""
    url_low = url.lower()
    for seg in BLACK_HOST:
        if seg in url_low:
            return True
    return False

def check_source_alive(url: str) -> tuple[bool, str]:
    """第一步：预校验源接口是否可访问，失效直接抛弃"""
    headers = {"User-Agent": "Mozilla/5.0 Android TV"}
    try:
        resp = requests.get(url, headers=headers, timeout=3)
        resp.raise_for_status()
        text = resp.text.strip()
        if len(text) < 20:
            return False, url
        return True, url
    except Exception:
        return False, url

def load_source_priority() -> list[str]:
    """读取sources，兼容中文「：」、英文「:」两种分隔，带（快）源优先抓取"""
    fast_group = []
    normal_group = []
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines() if l.strip() and not l.startswith("#")]
    for line in lines:
        # 自动识别分隔符
        if "：" in line:
            sep = "："
        elif ":" in line and line.count("http") == 1:
            sep = ":"
        else:
            normal_group.append(("", line))
            continue
        name, url = line.split(sep, 1)
        name = name.strip()
        url = url.strip()
        if "（快）" in name:
            fast_group.append((name, url))
        else:
            normal_group.append((name, url))
    # 高速源排在前面优先拉取
    all_source_pairs = fast_group + normal_group
    source_urls = [u for _, u in all_source_pairs]
    print(f"待检测源总数：{len(source_urls)}，标记高速源：{len(fast_group)}")

    # 批量校验源存活
    valid_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_CHECK_WORKER) as exe:
        results = exe.map(check_source_alive, source_urls)
    alive = 0
    dead = 0
    for ok, url in results:
        if ok:
            valid_urls.append(url)
            alive += 1
        else:
            dead += 1
    print(f"源校验完成：有效{alive}个，失效剔除{dead}个")
    return valid_urls

def fetch_all_channel_from_source(source_url: str) -> list[tuple[str, str]]:
    """抓取源内频道，兼容txt/m3u8，自动过滤三端不兼容链接"""
    channels = []
    headers = {"User-Agent": "Mozilla/5.0 Android TV"}
    try:
        resp = requests.get(source_url, headers=headers, timeout=4)
        resp.encoding = resp.apparent_encoding
        text = resp.text
        # 标准txt格式 频道名,url
        txt_reg = re.compile(r"([^,]+),(http[s]?://[^\n]+)")
        for ch_name, link in txt_reg.findall(text):
            ch_name = ch_name.strip().replace("#genre#", "").strip()
            link = link.strip()
            if ch_name and link and not is_incompatible_url(link):
                channels.append((ch_name, link))
        # m3u8 直播源格式
        m3u8_reg = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[s]?://[^\n]+)")
        for ch_name, link in m3u8_reg.findall(text):
            ch_name = ch_name.strip()
            link = link.strip()
            if ch_name and link and not is_incompatible_url(link):
                channels.append((ch_name, link))
    except Exception:
        pass
    return channels

def load_white_list() -> tuple[list[str], set[str]]:
    """读取白名单：返回【原始顺序列表】+ 快速匹配集合，保证输出不乱序"""
    white_order = []
    white_set = set()
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = line.strip()
            if line and not line.startswith("#"):
                white_order.append(line)
                white_set.add(line)
    print(f"白名单频道总数：{len(white_order)}")
    return white_order, white_set

def get_stream_resolution(stream_url: str) -> int:
    """获取流最大垂直分辨率；单流无分片信息默认720P"""
    headers = {"User-Agent": "Mozilla/5.0 Android TV"}
    try:
        resp = requests.get(stream_url, headers=headers, timeout=2)
        pl = m3u8.loads(resp.text)
        max_h = 0
        if pl.is_variant:
            for sub_pl in pl.playlists:
                if hasattr(sub_pl.stream_info, "resolution") and sub_pl.stream_info.resolution:
                    _, h = sub_pl.stream_info.resolution.split("x")
                    h = int(h)
                    if h > max_h:
                        max_h = h
        else:
            max_h = 720
        return max_h
    except Exception:
        return 0

def test_stream_delay(url: str) -> float:
    """测速函数，超时返回极大值9999"""
    headers = {"User-Agent": "Mozilla/5.0 Android TV"}
    start = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=TEST_TIMEOUT, stream=True)
        r.raw.read(512)
        delay = round(time.time() - start, 3)
        return delay
    except Exception:
        return 9999.0

def get_source_priority_score(url: str) -> int:
    """线路优先级：咪咕/央视频=0（最高），第三方普通源=1"""
    url_low = url.lower()
    # 咪咕域名标识
    if "migu" in url_low or "miguvideo" in url_low:
        return 0
    # 央视频/央视官方源标识
    if "cctv.cn" in url_low or "yangshipin" in url_low or "live.cctv" in url_low:
        return 0
    return 1

def main():
    # 1. 过滤失效源接口
    valid_source_urls = load_source_priority()
    if not valid_source_urls:
        print("无任何可用直播源，程序直接退出")
        return

    # 2. 读取白名单（保留原始输出顺序）
    white_order_list, white_set = load_white_list()

    # 3. 批量抓取所有频道，仅保留白名单内节目
    ch_link_map = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as exe:
        all_source_channels = exe.map(fetch_all_channel_from_source, valid_source_urls)
    for ch_list in all_source_channels:
        for ch_name, link in ch_list:
            if ch_name in white_set:
                ch_link_map[ch_name].append(link)

    # 4. 严格按照白名单从上到下顺序输出频道
    final_output = []
    for ch_name in white_order_list:
        if ch_name not in ch_link_map:
            continue
        url_list = ch_link_map[ch_name]
        temp_store = []

        # 并发测速 + 分辨率检测
        with concurrent.futures.ThreadPoolExecutor(max_workers=CHANNEL_TEST_WORKER) as exe:
            delay_res = list(exe.map(test_stream_delay, url_list))
            res_res = list(exe.map(get_stream_resolution, url_list))

        # 过滤 <720P 低清线路
        for link, delay, height in zip(url_list, delay_res, res_res):
            if height < MIN_VIDEO_HEIGHT:
                continue
            priority = get_source_priority_score(link)
            temp_store.append({
                "priority": priority,
                "delay": delay,
                "height": -height,  # 负号实现分辨率降序（同速度高清在前）
                "url": link
            })
        if not temp_store:
            continue

        # 多重排序权重（从高到低）
        # 1.咪咕/央视频官方源优先 → 2.延迟越小越快 → 3.分辨率越高
        temp_store.sort(key=lambda x: (x["priority"], x["delay"], x["height"]))
        # 只截取最优前3条线路
        top3_lines = temp_store[:MAX_PER_CHANNEL]
        for item in top3_lines:
            final_output.append(f"{ch_name},{item['url']}")

    # 写入UTF-8标准txt，TVBox/DIYP/vsTV直接导入无乱码
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(final_output))
    print(f"筛选完成！tv.txt 总有效线路：{len(final_output)}")

if __name__ == "__main__":
    main()
