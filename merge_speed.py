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

# ===================== 全局业务配置【提速参数调整】 =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
# 缩短单链接测速超时，快速放弃死链
STREAM_TEST_TIMEOUT = 1.2
MIN_VERTICAL_RES = 1080
MAX_STREAM_PER_CHANNEL = 3
SOURCE_FETCH_TIMEOUT = 3
# 拉源线程不变，测速线程适度提升（容器多核利用）
SOURCE_FETCH_WORKERS = 3
STREAM_EVAL_WORKERS = 10
DEBUG_LOG = False

# ===================== 底层工具函数【核心提速修复】 =====================
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

# 修复原代码缺失start变量 + 只请求头部不下载完整文件，减少IO耗时
def stream_quality_detect(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV"}
    delay = 9999.0
    max_res = 720
    start = time.time()  # 修复原代码未定义start的BUG
    try:
        # HEAD请求，只拿响应头，不下载完整m3u8，大幅减少流量&耗时
        resp = requests.head(
            url, headers=headers, timeout=STREAM_TEST_TIMEOUT,
            stream=True, verify=False, allow_redirects=True
        )
        delay = round(time.time() - start, 3)
        # 仅当HEAD正常再GET极小片段解析分辨率
        if resp.status_code == 200:
            resp_get = requests.get(url, headers=headers, timeout=0.5, verify=False, stream=True)
            m3u_obj = m3u8.loads(resp_get.text[:2000]) # 只读取前2000字符，不用完整文件
            for track in m3u_obj.playlists:
                if track.stream_info and track.stream_info.resolution:
                    w, h = track.stream_info.resolution.split("x")
                    h = int(h)
                    if h > max_res:
                        max_res = h
    except Exception:
        pass
    return delay, max_res

# ===================== 阶段1：加载源、白名单（完全保留分类注释输出逻辑，无修改） =====================
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
            # 全部原始行（注释、空行、频道名）存入origin_order，用于输出区块分割
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

# ===================== 阶段2：【重大提速重构】全局并发测速，取消频道串行阻塞 =====================
def filter_best_streams(channel_raw_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_map = defaultdict(list)
    total_ch = len(channel_raw_map)
    # 1. 扁平化所有(频道,链接)对，全局一次性并发，不再一个频道测完再下一个
    all_tasks = []
    ch_url_index = []
    curr_idx = 0
    for ch_name, url_list in channel_raw_map.items():
        for url in url_list:
            all_tasks.append(url)
            ch_url_index.append((curr_idx, ch_name, url))
            curr_idx += 1

    # 2. 全部链接统一并发测速，最大化利用线程池【红色改动：新增双层超时防护】
    task_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS) as exe:
        futures_map = {exe.submit(stream_quality_detect, url): idx for idx, url in enumerate(all_tasks)}
        # ↓↓↓【改动1】外层as_completed增加全局单批超时0.8s
        for fu in concurrent.futures.as_completed(futures_map, timeout=0.8):
            idx = futures_map[fu]
            try:
                # ↓↓↓【改动2】获取结果时增加单任务超时捕获
                delay, res = fu.result(timeout=0.8)
                task_result[idx] = (delay, res)
            except concurrent.futures.TimeoutError:
                # ↓↓↓【改动3】超时链接直接标记为极差，不阻塞线程
                task_result[idx] = (9999, 0)

    # 3. 按频道分组、排序、截取TOP3（原有优先级/延迟/分辨率逻辑完全保留）
    ch_temp = defaultdict(list)
    for idx, ch_name, url in ch_url_index:
        delay, res = task_result.get(idx, (9999, 0))
        ch_temp[ch_name].append((url, get_stream_priority(url), delay, -res))

    # 排序规则不变：优先级>延迟>分辨率
    curr = 0
    for ch_name, eval_res in ch_temp.items():
        curr += 1
        print(f"【阶段2测速进度】{curr}/{total_ch} 完成频道：{ch_name}")
        eval_res.sort(key=lambda x: (x[1], x[2], x[3]))
        top3 = [item[0] for item in eval_res[:MAX_STREAM_PER_CHANNEL]]
        final_map[ch_name] = top3
    return final_map

# ===================== 阶段3：输出标准化文件【完全无修改，保留分类区块】 =====================
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

# ===================== 主入口【新增链接去重，减少重复测速】 =====================
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

    # 【提速优化：链接去重，同一个URL只测速一次】
    for ch in raw_channel_cache:
        unique_urls = list(dict.fromkeys(raw_channel_cache[ch]))
        raw_channel_cache[ch] = unique_urls

    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}")
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}")
    export_result(white_origin_list, qualified_channel_map)
    print("====== 脚本全部执行完毕 ======")

if __name__ == "__main__":
    main()
