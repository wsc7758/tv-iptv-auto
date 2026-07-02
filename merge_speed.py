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
# 全局关闭SSL不安全警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 全局业务配置（调低并发，缩短超时，关闭调试打印） =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
STREAM_TEST_TIMEOUT = 2.0
MIN_VERTICAL_RES = 720
MAX_STREAM_PER_CHANNEL = 3
SOURCE_FETCH_TIMEOUT = 4
# 降低并发，防止网络拥塞卡死
SOURCE_FETCH_WORKERS = 3
STREAM_EVAL_WORKERS = 6
DEBUG_LOG = False

# ===================== 底层工具函数 =====================
def is_stream_incompatible(url: str) -> bool:
    ban_list = {"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://"}
    lower_url = url.lower()
    for keyword in ban_list:
        if keyword in lower_url:
            return True
    return False

def get_stream_priority(url: str) -> int:
    lower_url = url.lower()
    if "migu" in lower_url or "miguvideo" in lower_url:
        return 0
    if "cctv.cn" in lower_url or "yangshipin" in lower_url:
        return 0
    return 1

def stream_quality_detect(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV"}
    delay = 9999.0
    max_res = 720
    try:
        resp = requests.get(url, headers=headers, timeout=STREAM_TEST_TIMEOUT, stream=True, verify=False)
        delay = round(time.time() - start, 3)
        m3u_obj = m3u8.loads(resp.text)
        for track in m3u_obj.playlists:
            if track.stream_info and track.stream_info.resolution:
                w, h = track.stream_info.resolution.split("x")
                h = int(h)
                if h > max_res:
                    max_res = h
    except Exception:
        pass
    return delay, max_res

# ===================== 阶段1：加载源、白名单（仅大小写兼容，移除复杂清洗规避KeyError） =====================
def load_source_list() -> list[str]:
    source_list = []
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            line = line.strip()
            if line and not line.startswith("#"):
                if "：" in line:
                    _, link = line.split("：", 1)
                    source_list.append(link.strip())
                elif ":" in line and "http" in line:
                    _, link = line.split(":", 1)
                    source_list.append(link.strip())
                else:
                    source_list.append(line.strip())
    print(f"【阶段1-源池加载】待拉取直播源节点总数：{len(source_list)}")
    return source_list

def load_white_list() -> tuple[list[str], set[str]]:
    origin_order = []
    lower_match_set = set()
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            raw_line = line.rstrip("\n")
            # 全部原始行（注释、空行、频道名）存入origin_order，用于输出排版
            origin_order.append(raw_line)
            strip_line = raw_line.strip()
            # 仅纯频道加入匹配集合，注释/空行跳过匹配
            if strip_line and not raw_line.startswith("#"):
                lower_match_set.add(strip_line.lower())
    print(f"【阶段1-白名单加载】基准频道总数量：{len(origin_order)}")
    return origin_order, lower_match_set

def fetch_channel_from_source(src_link: str, white_lower_set: set[str]) -> list[tuple[str, str]]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }
    result_pairs = []
    if src_link.startswith("//"):
        src_link = "https:" + src_link
    try:
        resp = requests.get(src_link, headers=headers, timeout=SOURCE_FETCH_TIMEOUT, verify=False)
        text = resp.text
        if DEBUG_LOG:
            print(f"【调试】源 {src_link} 拉取成功，文本长度 {len(text)}")
        # TXT 格式 频道名,url
        txt_pattern = re.compile(r"([^,]+),(https?://[^\n]+)")
        for ch_name, stream_url in txt_pattern.findall(text):
            ch_name = ch_name.strip().replace("#genre#", "")
            stream_url = stream_url.strip()
            if ch_name.startswith("#"):
                continue
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
                if DEBUG_LOG:
                    print(f"【调试-TXT匹配】{ch_name}")
        # M3U8 EXTINF 格式
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(https?://[^\n]+)")
        for ch_name, stream_url in m3u_pattern.findall(text):
            ch_name = ch_name.strip()
            stream_url = stream_url.strip()
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
                if DEBUG_LOG:
                    print(f"【调试-M3U匹配】{ch_name}")
    except Exception as e:
        if DEBUG_LOG:
            print(f"【调试】源 {src_link} 拉取异常：{str(e)}")
    if DEBUG_LOG:
        print(f"【调试】该源匹配有效频道数：{len(result_pairs)}")
    return result_pairs

# ===================== 阶段2：并发测速、筛选最优流 =====================
def filter_best_streams(channel_raw_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_map = defaultdict(list)
    total_ch = len(channel_raw_map)
    curr = 0
    for ch_name, url_list in channel_raw_map.items():
        curr += 1
        print(f"【阶段2测速进度】{curr}/{total_ch} 正在测速频道：{ch_name}")
        task_list = url_list
        eval_res = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as exe:
            futures = [exe.submit(stream_quality_detect, u) for u in task_list]
            for idx, fu in enumerate(futures):
                delay, res = fu.result()
                eval_res.append((url_list[idx], get_stream_priority(url_list[idx]), delay, -res))
        # 优先级优先，延迟升序，分辨率降序
        eval_res.sort(key=lambda x: (x[1], x[2], x[3]))
        top3 = [item[0] for item in eval_res[:MAX_STREAM_PER_CHANNEL]]
        final_map[ch_name] = top3
    return final_map

# ===================== 阶段3：输出标准化文件 =====================
def export_result(white_origin: list[str], final_stream_map: dict[str, list[str]]):
    lines = []
    for item in white_origin:
        # 分类注释、空白行直接原样写入，实现区块分割
        if item.startswith("#") or item.strip() == "":
            lines.append(item)
            continue
        ch_name = item.strip()
        # 有测速有效链接才输出频道+url
        if ch_name in final_stream_map and len(final_stream_map[ch_name]) > 0:
            for link in final_stream_map[ch_name]:
                lines.append(f"{ch_name},{link}")
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    stream_count = sum(1 for line in lines if "," in line)
    print(f"【阶段3-输出完成】最终有效流媒体总条数：{stream_count}")

# ===================== 主入口（补齐并发timeout、异常捕获，无语法错误） =====================
def main():
    # 启动第一行立刻打印，确认脚本已运行
    print("====== IPTV分拣脚本启动 ======")
    source_pool = load_source_list()
    white_origin_list, white_lower_set = load_white_list()
    raw_channel_cache = defaultdict(list)
    # 并发拉取源，增加timeout兜底，捕获超时异常
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_FETCH_WORKERS) as exe:
        task_list = source_pool
        futures = [exe.submit(fetch_channel_from_source, s, white_lower_set) for s in task_list]
        for fu in futures:
            try:
                pair_list = fu.result(timeout=SOURCE_FETCH_TIMEOUT + 2)
                for ch, link in pair_list:
                    raw_channel_cache[ch].append(link)
            except concurrent.futures.TimeoutError:
                print("【警告】单个直播源拉取超时，自动跳过")
            except Exception as e:
                print(f"【警告】直播源处理异常：{str(e)}")
    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}")
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}")
    export_result(white_origin_list, qualified_channel_map)
    print("====== 脚本全部执行完毕 ======")

if __name__ == "__main__":
    main()
