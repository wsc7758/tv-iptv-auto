import sys
sys.stdout.reconfigure(encoding="utf-8")
import requests
import concurrent.futures
import re
from collections import defaultdict
import time
import m3u8
import urllib3
import threading
import os
import datetime
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 全局分批参数完整保留
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
STREAM_TEST_TIMEOUT = 0.8
MIN_VERTICAL_RES = 1080
MAX_STREAM_PER_CHANNEL = 3
SOURCE_FETCH_TIMEOUT = 3
SOURCE_FETCH_WORKERS = 3
STREAM_EVAL_WORKERS = 6
BATCH_MAX_RUN_SEC = 25
batch_size = 60
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

# 内层请求0.8秒单链接超时
def stream_quality_detect(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV"}
    delay = 9999.0
    max_res = 720
    start = time.time()
    try:
        resp = requests.head(url, headers=headers, timeout=0.8, stream=True, verify=False, allow_redirects=True)
        delay = round(time.time() - start, 3)
        if resp.status_code == 200:
            resp_get = requests.get(url, headers=headers, timeout=0.8, verify=False, stream=True)
            m3u_obj = m3u8.loads(resp_get.text[:2000])
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
    print(f"【阶段1-源池加载】待拉取直播源节点总数：{len(source_list)}", flush=True)
    return source_list

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
    print(f"【阶段1-白名单加载】基准频道总数量：{len(origin_order)}", flush=True)
    return origin_order, lower_match_set

def fetch_channel_from_source(src_link: str, white_lower_set: set[str]) -> list[tuple[str, str]]:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}
    result_pairs = []
    if src_link.startswith("//"):
        src_link = "https:" + src_link
    try:
        resp = requests.get(src_link, headers=headers, timeout=SOURCE_FETCH_TIMEOUT, verify=False)
        text = resp.text
        txt_pattern = re.compile(r"([^,]+),(https?://[^\n]+)")
        for ch_name, stream_url in txt_pattern.findall(text):
            ch_name = ch_name.strip().replace("#genre#", "")
            stream_url = stream_url.strip()
            if ch_name.startswith("#"):
                continue
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(https?://[^\n]+)")
        for ch_name, stream_url in m3u_pattern.findall(text):
            ch_name = ch_name.strip()
            stream_url = stream_url.strip()
            ch_lower = ch_name.lower()
            if ch_lower in white_lower_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
    except Exception as e:
        if DEBUG_LOG:
            print(f"【调试】源 {src_link} 拉取异常：{str(e)}", flush=True)
    return result_pairs

# 60链接一批、25秒批次超时，超时保留已测速链接逻辑完整保留
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
    print(f"【测速预加载】待测速总链接数量：{total_url}", flush=True)

    for start in range(0, total_url, batch_size):
        batch_urls = all_tasks[start:start + batch_size]
        batch_end_idx = min(start + batch_size, total_url)
        print(f"【测速批次】{start+1} ~ {batch_end_idx} / {total_url}，单批限时{BATCH_MAX_RUN_SEC}秒", flush=True)
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
                print(f"【警告】本批次 {start+1} ~ {batch_end_idx} 运行超过{BATCH_MAX_RUN_SEC}秒，截断，仅保留已完成链接，跳过剩余未测速URL", flush=True)
                for fut in batch_fut_map:
                    if not fut.done():
                        fut.cancel()
        finally:
            exe.shutdown(wait=False)

    ch_temp = defaultdict(list)
    for idx, ch_name, url in ch_url_index:
        delay, res = task_result.get(idx, (9999, 0))
        ch_temp[ch_name].append((url, get_stream_priority(url), delay, -res))
    curr = 0
    for ch_name, eval_res in ch_temp.items():
        curr += 1
        print(f"【阶段2测速进度】{curr}/{total_ch} 完成频道：{ch_name}", flush=True)
        eval_res.sort(key=lambda x: (x[1], x[2], x[3]))
        top3 = [item[0] for item in eval_res[:MAX_STREAM_PER_CHANNEL]]
        final_map[ch_name] = top3
    return final_map

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
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        # 写入时间戳，保证每次文件内容变更，git可提交
        f.write(f"\n# 流水线自动生成更新时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        f.flush()
    # 强制磁盘落盘
    os.sync()
    stream_count = sum(1 for line in lines if "," in line)
    print(f"【阶段3-输出完成】最终有效流媒体总条数：{stream_count}", flush=True)

def main():
    print("====== IPTV分拣脚本启动 ======", flush=True)
    source_pool = load_source_list()
    white_origin_list, white_lower_set = load_white_list()
    raw_channel_cache = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_FETCH_WORKERS) as exe:
        task_list = source_pool
        futures = [exe.submit(fetch_channel_from_source, s, white_lower_set) for s in task_list]
        for fu in futures:
            try:
                pair_list = fu.result(timeout=SOURCE_FETCH_TIMEOUT + 2)
                for ch, link in pair_list:
                    raw_channel_cache[ch].append(link)
            except concurrent.futures.TimeoutError:
                print("【警告】单个直播源拉取超时，自动跳过", flush=True)
            except Exception as e:
                print(f"【警告】直播源处理异常：{str(e)}", flush=True)
    # 链接全局去重
    for ch in raw_channel_cache:
        unique_urls = list(dict.fromkeys(raw_channel_cache[ch]))
        raw_channel_cache[ch] = unique_urls
    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}", flush=True)
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}", flush=True)
    export_result(white_origin_list, qualified_channel_map)
    print("====== 脚本全部执行完毕 ======", flush=True)

    # 1. Python层线程等待15秒
    wait_max = 15
    wait_cnt = 0
    while wait_cnt < wait_max:
        alive_threads = [t for t in threading.enumerate() if t != threading.current_thread()]
        if len(alive_threads) == 0:
            break
        wait_cnt += 1
        print(f"等待子线程销毁 {wait_cnt}/{wait_max}", flush=True)
        time.sleep(1)

    # 2. 新增：强制清空urllib3底层连接池，释放C层网络线程（解决进程挂死核心）
    http_pool = urllib3.PoolManager()
    http_pool.clear()
    # 二次磁盘同步兜底
    os.sync()
    time.sleep(2)
    print("====== Python资源全部释放完成 ======", flush=True)

if __name__ == "__main__":
    main()
