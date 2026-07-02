import sys
import os
import time
import requests
import concurrent.futures
import re
from collections import defaultdict
import m3u8
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================== 全局优化参数 =====================
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
STREAM_TEST_TIMEOUT = 1.0
MIN_VERTICAL_RES = 1080
MAX_STREAM_PER_CHANNEL = 6
SOURCE_FETCH_TIMEOUT = 3
SOURCE_FETCH_WORKERS = 3
STREAM_EVAL_WORKERS = 4
BATCH_MAX_RUN_SEC = 40
batch_size = 40
DEBUG_LOG = False

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

# GET探测，无HEAD防盗链误杀，完整读取m3u8
def stream_quality_detect(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV"}
    delay = 9999.0
    max_res = 720
    start = time.time()
    try:
        resp = requests.get(
            url, headers=headers, timeout=STREAM_TEST_TIMEOUT,
            stream=True, verify=False, allow_redirects=True
        )
        delay = round(time.time() - start, 3)
        if resp.status_code == 200:
            m3u_full = resp.text
            m3u_obj = m3u8.loads(m3u_full)
            for track in m3u_obj.playlists:
                if track.stream_info and track.stream_info.resolution:
                    w, h = track.stream_info.resolution.split("x")
                    h = int(h)
                    if h > max_res:
                        max_res = h
    except Exception:
        pass
    return delay, max_res

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

# 完整保留白名单注释、空行、原始顺序，用于分区块输出
def load_white_list() -> tuple[list[str], set[str]]:
    origin_order = []
    lower_match_set = set()
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            raw_line = line.rstrip("\n")
            origin_order.append(raw_line)
            strip_line = raw_line.strip()
            if strip_line and not raw_line.startswith("#"):
                lower_match_set.add(strip_line.lower())
    print(f"【阶段1-白名单加载】基准频道总数量：{len(origin_order)}")
    return origin_order, lower_match_set

# 兼容txt、m3u8、flv三种直播格式
def fetch_channel_from_source(src_link: str, white_lower_set: set[str]) -> list[tuple[str, str]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}
    result_pairs = []
    if src_link.startswith("//"):
        src_link = "https:" + src_link
    try:
        resp = requests.get(src_link, headers=headers, timeout=SOURCE_FETCH_TIMEOUT, verify=False)
        text = resp.text
        # TXT
        txt_pattern = re.compile(r"([^,]+),(https?://[^\n]+)")
        for ch_name, stream_url in txt_pattern.findall(text):
            ch_name = ch_name.strip().replace("#genre#", "")
            stream_url = stream_url.strip()
            if ch_name.startswith("#"):
                continue
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
        # M3U8
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(https?://[^\n]+)")
        for ch_name, stream_url in m3u_pattern.findall(text):
            ch_name = ch_name.strip()
            stream_url = stream_url.strip()
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
        # FLV低延迟流
        flv_pattern = re.compile(r"([^,]+),(https?://[^\n]+\.flv)")
        for ch_name, stream_url in flv_pattern.findall(text):
            ch_name = ch_name.strip()
            stream_url = stream_url.strip()
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
    except Exception as e:
        if DEBUG_LOG:
            print(f"【调试】源 {src_link} 拉取异常：{str(e)}")
    return result_pairs

# 分批测速 + 批次超时自动截断 + 取消残留线程，不会卡死循环
def filter_best_streams(channel_raw_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_map = defaultdict(list)
    total_ch = len(channel_raw_map)
    all_tasks = []
    ch_url_index = []
    curr_idx = 0
    for ch_name, url_list in channel_raw_map.items():
        for url in url_list:
            all_tasks.append(url)
            ch_url_index.append((curr_idx, ch_name, url))
            curr_idx += 1
    task_result = {}
    total_url = len(all_tasks)
    print(f"【测速预加载】待测速总链接数量：{total_url}")
    for start in range(0, total_url, batch_size):
        batch_urls = all_tasks[start:start+batch_size]
        batch_end_idx = min(start + batch_size, total_url)
        print(f"【测速批次】{start+1} ~ {batch_end_idx} / {total_url}，单批限时{BATCH_MAX_RUN_SEC}秒")
        batch_fut_map = {}
        exe = concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS)
        try:
            for sub_idx, url in enumerate(batch_urls):
                real_idx = start + sub_idx
                fut = exe.submit(stream_quality_detect, url)
                batch_fut_map[fut] = real_idx
            try:
                for fu in concurrent.futures.as_completed(batch_fut_map, timeout=BATCH_MAX_RUN_SEC):
                    real_idx = batch_fut_map[fu]
                    try:
                        delay, res = fu.result(timeout=STREAM_TEST_TIMEOUT)
                        task_result[real_idx] = (delay, res)
                    except concurrent.futures.TimeoutError:
                        task_result[real_idx] = (9999, 0)
                    except Exception:
                        task_result[real_idx] = (9999, 0)
            except concurrent.futures.TimeoutError:
                print(f"【警告】本批次 {start+1} ~ {batch_end_idx} 运行超过{BATCH_MAX_RUN_SEC}秒，截断，仅保留已完成链接，跳过剩余未测速URL")
                # 取消所有未完成任务，解除线程阻塞
                for fut in batch_fut_map:
                    if not fut.done():
                        fut.cancel()
        finally:
            exe.shutdown(wait=False)
    # 按频道分组
    ch_temp = defaultdict(list)
    for idx, ch_name, url in ch_url_index:
        delay, res = task_result.get(idx, (9999, 0))
        ch_temp[ch_name].append((url, get_stream_priority(url), delay, -res))
    # 排序：咪咕/央视优先 > 分辨率优先 > 延迟靠后
    curr = 0
    for ch_name, eval_res in ch_temp.items():
        curr += 1
        print(f"【阶段2测速进度】{curr}/{total_ch} 完成频道：{ch_name}")
        eval_res.sort(key=lambda x: (x[1], -x[3], x[2]))
        topN = [item[0] for item in eval_res[:MAX_STREAM_PER_CHANNEL]]
        final_map[ch_name] = topN
    return final_map

# 写入立刻flush/close/sync，杜绝文件锁
def export_result(white_origin: list[str], final_stream_map: dict[str, list[str]]):
    lines = []
    for item in white_origin:
        if item.startswith("#") or item.strip() == "":
            lines.append(item)
            continue
        ch_name = item.strip()
        if ch_name in final_stream_map and len(final_stream_map[ch_name]) > 0:
            for link in final_stream_map[ch_name]:
                lines.append(f"{ch_name},{link}")
    f = open(OUTPUT_TXT, "w", encoding="utf-8")
    f.write("\n".join(lines))
    f.flush()
    f.close()
    os.sync()
    stream_count = sum(1 for line in lines if "," in line)
    print(f"【阶段3-输出完成】最终有效流媒体总条数：{stream_count}")

def main():
    print("====== IPTV分拣脚本启动 ======")
    source_pool = load_source_list()
    white_origin_list, white_lower_set = load_white_list()
    raw_channel_cache = defaultdict(list)
    # 拉源并发
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
    # 链接去重，减少重复测速
    for ch in raw_channel_cache:
        unique_urls = list(dict.fromkeys(raw_channel_cache[ch]))
        raw_channel_cache[ch] = unique_urls
    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}")
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}")
    export_result(white_origin_list, qualified_channel_map)
    print("====== 脚本全部执行完毕 ======")

    # 双重同步+加长休眠兜底
    for var in locals().values():
        if hasattr(var, "close") and callable(var.close):
            try:
                var.close()
            except Exception:
                pass
    os.sync()
    time.sleep(2)
    os.sync()
    time.sleep(5)

if __name__ == "__main__":
    main()
