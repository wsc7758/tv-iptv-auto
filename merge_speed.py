import sys
sys.stdout.reconfigure(encoding="utf-8")
import requests
import concurrent.futures
import re
from collections import defaultdict
import time
import urllib3
import os
import datetime
import threading

# 全局禁用长连接，单次请求用完立刻销毁socket
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def no_reuse_conn(self, timeout=None):
    return self._new_conn()
urllib3.connectionpool.ConnectionPool._get_conn = no_reuse_conn

# 全局业务参数
SOURCE_FILE = "sources.txt"
WHITE_LIST_FILE = "channel_whitelist.txt"
OUTPUT_TXT = "tv.txt"
STREAM_REQ_TIMEOUT = 1.5
TASK_GLOBAL_TIMEOUT = 12
BATCH_GLOBAL_TIMEOUT = 25
MIN_VERTICAL_RES = 1080
MAX_STREAM_PER_CHANNEL = 6
SOURCE_FETCH_TIMEOUT = 3
SOURCE_FETCH_WORKERS = 3
STREAM_EVAL_WORKERS = 12
batch_size = 60
DEBUG_LOG = False

def is_stream_incompatible(url: str) -> bool:
    ban_list = {"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://", "rtsp://", "srt://", "udp://", "tcp://"}
    lower_url = url.lower()
    if not (url.startswith("http://") or url.startswith("https://")):
        return True
    return any(key in lower_url for key in ban_list)

def get_stream_priority(url: str) -> int:
    lower_url = url.lower()
    if "migu" in lower_url or "miguvideo" in lower_url:
        return 0
    if "cctv.cn" in lower_url or "yangshipin" in lower_url:
        return 0
    return 1

def stream_quality_detect(url: str) -> tuple[float, int]:
    headers = {"User-Agent": "Mozilla/5.0 AndroidTV", "Connection": "close"}
    delay = 9999.0
    max_res = 0
    start = time.time()
    try:
        resp = requests.head(
            url,
            headers=headers,
            timeout=STREAM_REQ_TIMEOUT,
            stream=True,
            verify=False,
            allow_redirects=True
        )
        delay = round(time.time() - start, 3)
        # 2xx/3xx 全部判定有效流，兼容跳转CCTV源
        if 200 <= resp.status_code < 400:
            max_res = 1080
    except Exception:
        pass
    return delay, max_res

def batch_subtask(url_group: list[tuple[int, str, str]]) -> dict[int, tuple[float, int]]:
    task_start = time.time()
    local_result = {}
    for real_idx, ch_name, url in url_group:
        if time.time() - task_start >= TASK_GLOBAL_TIMEOUT:
            break
        local_result[real_idx] = stream_quality_detect(url)
    return local_result

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

# 修复：完整清洗每行换行/空格，解决#genre#分组识别失败丢失CCTV
def load_white_list() -> tuple[list, dict]:
    group_info = []
    channel_to_group = dict()
    current_group = ""
    with open(WHITE_LIST_FILE, "r", encoding="utf-8") as f:
        for line in f.readlines():
            # 彻底清除\r \n 首尾空格
            raw_line = line.replace("\r","").replace("\n","").strip()
            if not raw_line:
                continue
            if raw_line.endswith(",#genre#"):
                current_group = raw_line.replace(",#genre#", "").strip()
                group_info.append((current_group, []))
                continue
            if current_group:
                clean_ch = raw_line.strip()
                group_info[-1][1].append(clean_ch)
                channel_to_group[clean_ch] = current_group
    print(f"【阶段1-白名单加载】共读取分类数量：{len(group_info)}，白名单频道总数：{len(channel_to_group)}", flush=True)
    # 打印所有白名单频道，校验CCTV是否存在
    print(f"【白名单全频道列表】{list(channel_to_group.keys())}", flush=True)
    return group_info, channel_to_group

def fetch_channel_from_source(src_link: str, white_channel_set: set[str]) -> list[tuple[str, str]]:
    headers = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0 Safari/537.36", "Connection": "close"}
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
            if ch_name in white_channel_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(https?://[^\n]+)")
        for ch_name, stream_url in m3u_pattern.findall(text):
            ch_name = ch_name.strip()
            stream_url = stream_url.strip()
            if ch_name in white_channel_set and not is_stream_incompatible(stream_url):
                result_pairs.append((ch_name, stream_url))
    except Exception as e:
        if DEBUG_LOG:
            print(f"【调试】源 {src_link} 拉取异常：{str(e)}", flush=True)
    return result_pairs

def filter_best_streams(channel_raw_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_map = defaultdict(list)
    total_ch = len(channel_raw_map)
    ch_url_index = []
    curr_idx = 0
    # 打印所有待测速频道，确认CCTV是否进入测速队列
    print(f"【全部待测速频道列表】{list(channel_raw_map.keys())}", flush=True)
    for ch_name, url_list in channel_raw_map.items():
        for url in url_list:
            ch_url_index.append((curr_idx, ch_name, url))
            curr_idx += 1
    task_result = {}
    total_url = len(ch_url_index)
    print(f"【测速预加载】待测速总链接数量：{total_url}", flush=True)

    for start in range(0, total_url, batch_size):
        batch_start_time = time.time()
        batch_items = ch_url_index[start:start + batch_size]
        batch_end_idx = min(start + batch_size, total_url)
        print(f"【测速批次】{start+1} ~ {batch_end_idx} / {total_url}", flush=True)
        sub_task_groups = [[] for _ in range(STREAM_EVAL_WORKERS)]
        for idx, item in enumerate(batch_items):
            sub_task_groups[idx % STREAM_EVAL_WORKERS].append(item)
        exe = concurrent.futures.ThreadPoolExecutor(max_workers=STREAM_EVAL_WORKERS)
        futures = []
        try:
            futures = [exe.submit(batch_subtask, g) for g in sub_task_groups if g]
            complete_futures = set()
            while len(complete_futures) < len(futures):
                if time.time() - batch_start_time >= BATCH_GLOBAL_TIMEOUT:
                    print(f"【批次超时】本批运行已满25秒，终止剩余未完成测速，已测数据全部保留", flush=True)
                    urllib3.PoolManager().clear()
                    break
                try:
                    for fu in concurrent.futures.as_completed(futures, timeout=0.3):
                        if fu not in complete_futures:
                            complete_futures.add(fu)
                            try:
                                task_result.update(fu.result())
                            except Exception as e:
                                print(f"【线程异常】本组线程出错，已测数据保留：{str(e)}", flush=True)
                except concurrent.futures.TimeoutError:
                    continue
        finally:
            exe.shutdown(wait=True, cancel_futures=False)
            urllib3.PoolManager().clear()
            pool_tmp = urllib3.PoolManager()
            pool_tmp.clear()
            print(f"【批次完成】{start+1}~{batch_end_idx} 批次资源回收完毕", flush=True)

    ch_temp = defaultdict(list)
    for idx, ch_name, url in ch_url_index:
        delay, res = task_result.get(idx, (9999, 0))
        ch_temp[ch_name].append((url, get_stream_priority(url), delay, -res))
    curr = 0
    for ch_name, eval_res in ch_temp.items():
        curr += 1
        print(f"【阶段2测速进度】{curr}/{total_ch} 完成频道：{ch_name}", flush=True)
        eval_res.sort(key=lambda x: (x[1], x[2], x[3]))
        qualified = [item for item in eval_res if -item[3] >= MIN_VERTICAL_RES]
        print(f"【频道统计】{ch_name} 达标链接总数：{len(qualified)}，单频道最大留存：{MAX_STREAM_PER_CHANNEL}", flush=True)
        final_map[ch_name] = [item[0] for item in qualified[:MAX_STREAM_PER_CHANNEL]]
    return final_map

# 输出函数：分类表头严格匹配 分类名,#genre#
def export_result(group_info: list, final_stream_map: dict[str, list[str]]):
    lines = []
    for group_name, ch_list in group_info:
        lines.append(f"{group_name},#genre#")
        for ch_name in ch_list:
            stream_list = final_stream_map.get(ch_name, [])
            for link in stream_list:
                lines.append(f"{ch_name},{link}")
        lines.append("")
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.flush()
    os.sync()
    stream_count = sum(1 for line in lines if "," in line)
    print(f"【阶段3-输出完成】最终有效流媒体总条数：{stream_count}", flush=True)

def main():
    print("====== IPTV分拣脚本启动 ======", flush=True)
    source_pool = load_source_list()
    group_info, channel_to_group = load_white_list()
    white_channel_set = set(channel_to_group.keys())
    raw_channel_cache = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_FETCH_WORKERS) as exe:
        futures = [exe.submit(fetch_channel_from_source, s, white_channel_set) for s in source_pool]
        for fu in futures:
            try:
                pair_list = fu.result(timeout=SOURCE_FETCH_TIMEOUT + 2)
                for ch, link in pair_list:
                    raw_channel_cache[ch].append(link)
            except concurrent.futures.TimeoutError:
                print("【警告】单个直播源拉取超时，自动跳过", flush=True)
            except Exception as e:
                print(f"【警告】直播源处理异常：{str(e)}", flush=True)
    for ch in raw_channel_cache:
        raw_channel_cache[ch] = list(dict.fromkeys(raw_channel_cache[ch]))
    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}", flush=True)
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}", flush=True)
    export_result(group_info, qualified_channel_map)

    urllib3.PoolManager().clear()
    os.sync()
    time.sleep(0.5)
    pool = urllib3.PoolManager()
    pool.clear()
    urllib3.disable_warnings()
    for t in threading.enumerate():
        if t is not threading.main_thread():
            t.join(timeout=1)
    time.sleep(1)
    print("====== Python资源全部释放完成 ======", flush=True)
    sys.exit(0)

if __name__ == "__main__":
    main()
