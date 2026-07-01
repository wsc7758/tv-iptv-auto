import requests
import concurrent.futures
import re
from collections import defaultdict
import time
import m3u8

# ===================== 配置区 =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
TEST_TIMEOUT = 2.0
MIN_HEIGHT = 720  # 低于720p直接剔除
MAX_KEEP = 3      # 每个频道保留最优3条
# 三端不兼容/内网黑名单
BLACK_URL_KEY = {"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://"}
# 并发控制
SOURCE_WORKERS = 6
STREAM_WORKERS = 10

# ===================== 工具函数 =====================
def is_bad_url(url: str) -> bool:
    """过滤TVBox/DIYP/vsTV无法播放链接"""
    ul = url.lower()
    for k in BLACK_URL_KEY:
        if k in ul:
            return True
    return False

def load_sources() -> list[str]:
    """【移除源预检测，直接返回全部源】不再校验是否存活，避免海外网络误杀"""
    fast_src = []
    normal_src = []
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    for line in lines:
        if "：" in line:
            name, link = line.split("：", 1)
        elif ":" in line and "http" in line:
            name, link = line.split(":", 1)
        else:
            normal_src.append(line.strip())
            continue
        name = name.strip()
        link = link.strip()
        if "（快）" in name:
            fast_src.append(link)
        else:
            normal_src.append(link)
    all_src = fast_src + normal_src
    print(f"跳过源预检测，待处理总源：{len(all_src)}")
    return all_src

def load_white_list() -> tuple[list[str], set[str]]:
    """读取白名单，保留原始输出顺序"""
    order_list = []
    name_set = set()
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f:
            ln = line.strip()
            if ln and not ln.startswith("#"):
                order_list.append(ln)
                name_set.add(ln)
    print(f"白名单频道总数：{len(order_list)}")
    return order_list, name_set

def fetch_valid_channels(src_url: str, white_set: set[str]) -> list[tuple[str, str]]:
    """拉取频道，直接过滤非白名单、不兼容链接"""
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }
    res_list = []
    try:
        text = requests.get(src_url, timeout=6, headers=headers).text
        # txt格式匹配
        txt_reg = re.compile(r"([^,]+),(http[s]?://[^\n]+)")
        for ch, link in txt_reg.findall(text):
            ch = ch.strip().replace("#genre#", "")
            link = link.strip()
            if ch in white_set and not is_bad_url(link):
                res_list.append((ch, link))
        # m3u8格式匹配
        m3u8_reg = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[s]?://[^\n]+)")
        for ch, link in m3u8_reg.findall(text):
            ch = ch.strip()
            link = link.strip()
            if ch in white_set and not is_bad_url(link):
                res_list.append((ch, link))
    except Exception:
        pass
    return res_list

def get_priority(url: str) -> int:
    """咪咕/央视频线路优先级最高 0 > 普通源1"""
    ul = url.lower()
    if "migu" in ul or "miguvideo" in ul:
        return 0
    if "cctv.cn" in ul or "live.cctv" in ul or "yangshipin" in ul:
        return 0
    return 1

def test_stream(url: str) -> tuple[float, int]:
    """合并测速+分辨率，一次请求完成；真正失效链接延迟=9999自动淘汰"""
    headers = {"User-Agent":"Mozilla/5.0 AndroidTV"}
    start = time.time()
    try:
        r = requests.get(url, timeout=TEST_TIMEOUT, headers=headers, stream=True)
        r.raw.read(256)
        delay = round(time.time() - start, 3)
        resp_text = r.text
    except Exception:
        delay = 9999.0
        resp_text = ""
    # 解析分辨率
    max_h = 720
    try:
        if resp_text:
            pl = m3u8.loads(resp_text)
            if pl.is_variant:
                max_h = 0
                for p in pl.playlists:
                    if hasattr(p.stream_info, "resolution") and p.stream_info.resolution:
                        _, h = p.stream_info.resolution.split("x")
                        max_h = max(max_h, int(h))
    except Exception:
        max_h = 720
    return delay, max_h

def main():
    # 步骤1：读取全部源，不再预校验存活
    all_sources = load_sources()
    if not all_sources:
        print("无任何直播源，程序退出")
        return
    # 加载白名单顺序
    white_order, white_set = load_white_list()
    # 步骤2：批量拉取仅白名单频道
    ch_map = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as exe:
        all_ch = exe.map(lambda s: fetch_valid_channels(s, white_set), all_sources)
    for item_list in all_ch:
        for ch_name, link in item_list:
            ch_map[ch_name].append(link)
    # 按白名单顺序遍历处理频道
    output_lines = []
    for ch_name in white_order:
        if ch_name not in ch_map:
            continue
        links = ch_map[ch_name]
        # 并发测速+分辨率
        test_results = list(concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_WORKERS).map(test_stream, links))
        temp = []
        for link, (delay, height) in zip(links, test_results):
            # 过滤规则：延迟超时(9999) 或 分辨率<720 直接丢弃
            if delay >= 9999 or height < MIN_HEIGHT:
                continue
            temp.append({
                "pri": get_priority(link),
                "delay": delay,
                "height": -height,
                "url": link
            })
        if not temp:
            continue
        # 多重排序：咪咕央视频优先 → 速度越快 → 画质越高
        temp.sort(key=lambda x: (x["pri"], x["delay"], x["height"]))
        # 保留最优3条
        top3 = temp[:MAX_KEEP]
        for item in top3:
            output_lines.append(f"{ch_name},{item['url']}")
    # 按白名单顺序输出tv.txt
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    print(f"筛选完成，最终有效播放线路：{len(output_lines)}")

if __name__ == "__main__":
    main()
