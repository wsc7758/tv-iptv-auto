import sys
# 强制标准输出UTF8，解决中文乱码
sys.stdout.reconfigure(encoding="utf-8")
import requests
import concurrent.futures
import re
from collections import defaultdict
import time
import m3u8
import urllib3
# 全局关闭SSL不安全警告，消除刷屏日志
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 全局业务配置 =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
STREAM_TEST_TIMEOUT = 2.0
MIN_VERTICAL_RES = 720
MAX_STREAM_PER_CH = 3
SOURCE_FETCH_TIMEOUT = 6
INCOMPATIBLE_FILTER = {"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://"}
SOURCE_FETCH_WORKERS = 6
STREAM_EVAL_WORKERS = 10
DEBUG_LOG = False  # 线上关闭调试日志，True本地排查开启

# ===================== 底层工具函数 =====================
def clean_channel_name(name: str) -> str:
    """清洗频道名：去除横杠、高清/4K/后缀，提升模糊匹配率"""
    name = name.strip().lower()
    # 清除无关后缀
    suffix_list = ["高清", "超清", "4k", "综合", "财经", "综艺", "电影", "纪录", "少儿", "音乐", "体育赛事", "国防军事"]
    for s in suffix_list:
        name = name.replace(s.lower(), "")
    # 清除横杠、空格
    name = name.replace("-", "").replace(" ", "")
    return name

def is_incompatible_stream(url: str) -> bool:
    url_lower = url.lower()
    for keyword in INCOMPATIBLE_FILTER:
        if keyword in url_lower:
            return True
    return False

def get_stream_priority(url: str) -> int:
    url_lower = url.lower()
    if "migu" in url or "miguvideo" in url:
        return 0
    if "cctv.cn" in url or "live.cctv" in url or "yangshipin" in url:
        return 0
    return 1

def unified_stream_evaluation(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV"}
    start_ts = time.time()
    delay = 9999.0
    resp_text = ""
    try:
        resp = requests.get(url, headers=headers, timeout=STREAM_TEST_TIMEOUT, stream=True, verify=False)
        resp.raw.read(256)
        delay = round(time.time() - start_ts, 3)
        resp_text = resp.text
    except Exception:
        pass
    max_vertical = 720
    try:
        if resp_text:
            playlist = m3u8.loads(resp_text)
            if playlist.is_variant:
                max_vertical = 0
                for sub_pl in playlist.playlists:
                    if hasattr(sub_pl.stream_info, "resolution") and sub_pl.stream_info.resolution:
                        _, h = sub_pl.stream_info.resolution.split("x")
                        max_vertical = max(max_vertical, int(h))
    except Exception:
        pass
    return delay, max_vertical

# ===================== 阶段1：源池拉取与白名单频道预过滤 =====================
def load_raw_source_pool() -> list[str]:
    fast_source_group = []
    normal_source_group = []
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]
    for line in raw_lines:
        if "：" in line:
            sep = "："
            name_part, url_part = line.split(sep, 1)
        elif ":" in line and "http" in line:
            sep = ":"
            name_part, url_part = line.split(sep, 1)
        else:
            normal_source_group.append(line.strip())
            continue
        name_part = name_part.strip()
        url_part = url_part.strip()
        if "（快）" in name_part:
            fast_source_group.append(url_part)
        else:
            normal_source_group.append(url_part)
    full_source_list = fast_source_group + normal_source_group
    print(f"【阶段1-源池加载】待拉取直播源节点总数：{len(full_source_list)}")
    return full_source_list

def load_white_list_spec() -> tuple[list[str], set[str], dict[str, str]]:
    """返回：原始有序列表、清洗后匹配集合、原始名-清洗名映射"""
    order_benchmark = []
    clean_match_set = set()
    name_mapping = {}
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            ln = line.strip()
            if ln and not ln.startswith("#"):
                order_benchmark.append(ln)
                clean_name = clean_channel_name(ln)
                clean_match_set.add(clean_name)
                name_mapping[clean_name] = ln
    print(f"【阶段1-白名单加载】基准频道序列总量：{len(order_benchmark)}")
    return order_benchmark, clean_match_set, name_mapping

def fetch_source_channel_index(src_url: str, white_clean_set: set[str], name_map: dict[str, str]) -> list[tuple[str, str]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0 Safari/537.36"
    }
    valid_pair = []
    if src_url.startswith("//"):
        src_url = "https:" + src_url
    try:
        resp = requests.get(src_url, headers=headers, timeout=SOURCE_FETCH_TIMEOUT, verify=False)
        text = resp.text
        if DEBUG_LOG:
            print(f"【调试】源地址 {src_url} 拉取成功，文本长度：{len(text)}")
        # TXT匹配
        txt_reg = re.compile(r"([^,]+),(http[s]?://[^\n]+)")
        for ch_name, link in txt_reg.findall(text):
            ch_name = ch_name.strip().replace("#genre#", "")
            link = link.strip()
            clean_ch = clean_channel_name(ch_name)
            if DEBUG_LOG:
                print(f"【调试-TXT提取原始名】{ch_name} 清洗后：{clean_ch}")
            # 过滤#标签、不兼容链接，模糊匹配白名单
            if clean_ch in white_clean_set and not is_incompatible_stream(link) and not ch_name.startswith("#"):
                real_ori_name = name_map[clean_ch]
                valid_pair.append((real_ori_name, link))
        # M3U8匹配
        m3u8_reg = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[s]?://[^\n]+)")
        for ch_name, link in m3u8_reg.findall(text):
            ch_name = ch_name.strip()
            link = link.strip()
            clean_ch = clean_channel_name(ch_name)
            if DEBUG_LOG:
                print(f"【调试-M3U8提取原始名】{ch_name} 清洗后：{clean_ch}")
            if clean_ch in white_clean_set and not is_incompatible_stream(link) and not ch_name.startswith("#"):
                real_ori_name = name_map[clean_ch]
                valid_pair.append((real_ori_name, link))
    except Exception as e:
        if DEBUG_LOG:
            print(f"【调试】源地址 {src_url} 拉取失败，异常详情：{str(e)}")
    if DEBUG_LOG:
        print(f"【调试】该源匹配白名单有效频道数量：{len(valid_pair)}")
    return valid_pair

# ===================== 阶段2：流媒体一体化质量评测与链路择优 =====================
def evaluate_and_filter_streams(ch_link_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_filter_map = defaultdict(list)
    for ch_name, url_list in ch_link_map.items():
        eval_results = list(concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS).map(unified_stream_evaluation, url_list))
        qualified_items = []
        for link, (delay, height) in zip(url_list, eval_results):
            if delay >= 9999 or height < MIN_VERTICAL_RES:
                continue
            item_score = {
                "priority": get_stream_priority(link),
                "delay": delay,
                "res_neg": -height,
                "url": link
            }
            qualified_items.append(item_score)
        if not qualified_items:
            continue
        qualified_items.sort(key=lambda x: (x["priority"], x["delay"], x["res_neg"]))
        top3_links = [item["url"] for item in qualified_items[:MAX_STREAM_PER_CH]]
        final_filter_map[ch_name] = top3_links
    return final_filter_map

# ===================== 阶段3：标准化清单持久化输出 =====================
def generate_output_file(white_order: list[str], filter_map: dict[str, list[str]]):
    output_rows = []
    for ch_name in white_order:
        if ch_name not in filter_map:
            continue
        for stream_url in filter_map[ch_name]:
            output_rows.append(f"{ch_name},{stream_url}")
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(output_rows))
    print(f"【阶段3-输出完成】最终合规流媒体链路总条数：{len(output_rows)}")

# ===================== 主入口 =====================
def main():
    source_pool = load_raw_source_pool()
    white_order_bench, white_clean_set, white_name_map = load_white_list_spec()
    channel_link_cache = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_FETCH_WORKERS) as exe:
        all_source_data = exe.map(lambda s: fetch_source_channel(s, white_clean_set, white_name_map), source_pool)
    for ch_pair_list in all_source_data:
        for ch, url in ch_pair_list:
            channel_link_cache[ch].append(url)
    print(f"【阶段1完成】预过滤后待评测频道数量：{len(channel_link_cache)}")
    qualified_links = evaluate_and_filter_streams(channel_link_cache)
    print(f"【阶段2完成】完成画质测速的有效频道数量：{len(qualified_links)}")
    generate_output_file(white_order_bench, qualified_links)

if __name__ == "__main__":
    main()
