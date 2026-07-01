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
# 并发控制，兼顾速度与Github限流
SOURCE_WORKERS = 6
STREAM_WORKERS = 10

# ===================== 工具函数 =====================
def is_bad_url(url: str) -> bool:
    """过滤无法在TVBox/DIYP/vsTV播放的链接"""
    ul = url.lower()
    for k in BLACK_URL_KEY:
        if k in ul:
            return True
    return False

def check_source_alive(url: str) -> tuple[bool, str]:
    """步骤1：校验源文件是否存活，失效直接丢弃"""
    headers = {"User-Agent":"Mozilla/5.0 AndroidTV"}
    try:
        resp = requests.get(url, timeout=3, headers=headers)
        resp.raise_for_status()
        if len(resp.text.strip()) < 10:
            return False, url
        return True, url
    except Exception:
        return False, url

def load_sources() -> list[str]:
    """读取sources，优先带（快）标记源，再批量校验存活（步骤1）"""
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
    # 批量检测失效源
    valid_src = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as exe:
        res = exe.map(check_source_alive, all_src)
    for ok, u in res:
        if ok:
            valid_src.append(u)
    print(f"步骤1完成：总源{len(all_src)}，有效源{len(valid_src)}")
    return valid

def load_white_list() -> tuple[list[str], set[str]]:
    """读取白名单：保存输出顺序+快速查询集合"""
    order_list = []
    name_set = set()
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f:
            ln = line.strip()
            if ln and not ln.startswith("#"):
                order_list.append(ln)
                name_set.add(ln)
    print(f"白名单频道总数：{len(order_list)}")
    return order_list, name

def fetch_valid_channels(src_url: str, white_set: set[str]) -> list[tuple[str, str]]:
    """合并：拉取频道 + 直接过滤非白名单（步骤2前置过滤，节省内存）"""
    headers = {"User-Agent":"Mozilla/5.0 AndroidTV"}
    res_list = []
    try:
        text = requests.get(src_url, timeout=4, headers=headers).text
        # 匹配 txt 频道,url
        txt_reg = re.compile(r"([^,]+),(http[s]?://[^\n]+)")
        for ch, link in txt_reg.findall(text):
            ch = ch.strip().replace("#genre#", "")
            link = link.strip()
            if ch in white_set and not is_bad_url(link):
                res_list.append((ch, link))
        # 匹配 m3u8
        m3u8_reg = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[s]?://[^\n]+)")
        for ch, link in m3u8_reg.findall(text):
            ch = ch.strip()
            link = link.strip()
            if ch in white_set and not is_bad_url(link):
                res_list.append((ch, link))
    except Exception:
        pass
    return res

def get_priority(url: str) -> int:
    """咪咕/央视频优先：0最高，普通1"""
    ul = url.lower()
    if "migu" in ul or "miguvideo" in ul:
        return 0
    if "cctv.cn" in ul or "live.cctv" in ul or "yangshipin" in ul:
        return 0
    return 1

def test_stream(url: str) -> tuple[float, int]:
    """合并测速+分辨率检测（步骤2+3合并，只发一次请求，大幅提速）"""
    headers = {"User-Agent":"Mozilla/5.0 AndroidTV"}
    # 测速
    start = time.time()
    try:
        r = requests.get(url, timeout=TEST_TIMEOUT, headers=headers, stream=True)
        r.raw.read(256)
        delay = round(time.time() - start, 3)
    except Exception:
        delay = 9999.0
    # 解析分辨率
    try:
        pl = m3u8.loads(r.text if delay < 999 else requests.get(url, timeout=2, headers=headers).text)
        max_h = 720
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
    # 步骤1：检测并过滤失效源
    valid_sources = load_sources()
    if not valid_sources:
        print("无有效直播源，退出")
        return
    # 加载白名单（输出顺序+过滤集合）
    white_order, white_set = load_white_list()
    # 步骤2：拉取频道，直接丢弃不在白名单的节目（合并过滤，减少数据量）
    ch_map = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as exe:
        all_ch = exe.map(lambda s: fetch_valid_channels(s, white_set), valid_sources)
    for item_list in all_ch:
        for ch_name, link in item_list:
            ch_map[ch_name].append(link)
    # 遍历白名单顺序生成输出（步骤5输出规则）
    output_lines = []
    for ch_name in white_order:
        if ch_name not in ch_map:
            continue
        links = ch_map[ch_name]
        # 并发一次完成【测速+分辨率】（步骤2+3合并，只一轮并发）
        test_results = list(concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_WORKERS).map(test_stream, links))
        temp = []
        for link, (delay, height) in zip(links, test_results):
            # 步骤3：剔除低于720P
            if height < MIN_HEIGHT:
                continue
            temp.append({
                "pri": get_priority(link),
                "delay": delay,
                "height": -height, # 负号实现高清在前
                "url": link
            })
        if not temp:
            continue
        # 统一一次排序：官方源>速度>清晰度
        temp.sort(key=lambda x: (x["pri"], x["delay"], x["height"]))
        # 步骤4：截取最优3条
        top3 = temp[:MAX_KEEP]
        for item in top3:
            output_lines.append(f"{ch_name},{item['url']}")
    # 步骤5：按白名单顺序写入文件
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    print(f"筛选完成，最终有效线路：{len(output_lines)}")

if __name__ == "__main__":
    main()
