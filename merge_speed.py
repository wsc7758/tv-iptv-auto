导入系统
sys.stdout.reconfigure(encoding='utf-8')stdout.reconfigure(encoding="utf-8")
导入 requests requests
导入concurrent.futures concurrent.futures
导入正则表达式模块 re
从集合中导入默认字典 collections import defaultdict
导入时间 time
导入m3u8 m3u8
导入urllib3 urllib3
导入操作系统模块 os
导入日期时间 datetime

# 全局禁用连接池长连接，强制每次请求销毁socket
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def no_reuse_conn(self, timeout=None): no_reuse_conn(self, timeout=None):
    返回 self._new_conn()return self._new_conn()
urllib3.connectionpool.ConnectionPool._get_conn = 不重复使用连接connectionpool.ConnectionPool._get_conn = no_reuse_conn

# 全局参数
SOURCE_FILE = "sources.txt""sources.txt"
WHITELIST_FILE = "channel_whitelist.txt""channel_whitelist.txt"
OUTPUT_TXT = "tv.txt""tv.txt"
STREAM_REQ_TIMEOUT = 1111
任务全局超时 = 12121212
BATCH_GLOBAL_TIMEOUT = 25252525
最小垂直分辨率 = 1080108010801080
每个通道的最大流数 = 3333
SOURCE_FETCH_TIMEOUT = 3333
SOURCE_FETCH_WORKERS = 3333
STREAM_EVAL_WORKER = 1212
批次大小 = 60606060
DEBUG_LOG = 假False

定义是否流不兼容（url: str） -> bool: 是流不兼容（url: str） -> bool: is_stream_incompatible(url: str) -> bool: is_stream_incompatible(url: str) -> bool:
    ban_list = {'127.', '192.168.', '10.', '172.', 'localhost', 'rtmp://', 'igmp://'}{"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://"}{'127.', '192.168.', '10.', '172.', 'localhost', 'rtmp://', 'igmp://'}{"127.", "192.168.", "10.", "172.", "localhost", "rtmp://", "igmp://"}
    lower_url = url.lower()lower()lower()
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
    max_res = 720
    start = time.time()
    try:
        resp = requests.head(url, headers=headers, timeout=STREAM_REQ_TIMEOUT, stream=True, verify=False, allow_redirects=True)
        delay = round(time.time() - start, 3)
        if resp.status_code == 200:
            resp_get = requests.get(url, headers=headers, timeout=STREAM_REQ_TIMEOUT, verify=False, stream=True)
            m3u_obj = m3u8.loads(resp_get.text[:2000])
            for track in m3u_obj.playlists:
                if track.stream_info and track.stream_info.resolution:
                    _, h = track.stream_info.resolution.split("x")
                    h = int(h)
                    if h > max_res:
                        max_res = h
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

def filter_best_streams(channel_raw_map: dict[str, list[str]]) -> dict[str, list[str]]:
    final_map = defaultdict(list)
    total_ch = len(channel_raw_map)
    ch_url_index = []
    curr_idx = 0
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
            exe.shutdown(wait=False, cancel_futures=True)
            urllib3.PoolManager().clear()
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
        final_map[ch_name] = [item[0] for item in eval_res[:MAX_STREAM_PER_CHANNEL]]
    return final_map

def export_result(white_origin: list[str], final_stream_map: dict[str, list[str]]):
    lines = []
    for item in white_origin:
        if item.startswith("#") or item.strip() == "":
            lines.append(item)
            continue
        ch_name = item.strip()
        if ch_name in final_stream_map and len(final_stream_map[ch_name]) > 0:
            lines.extend([f"{ch_name},{link}" for link in final_stream_map[ch_name]])
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write(f"\n# 流水线自动生成更新时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        f.flush()
    os.sync()
    stream_count = sum(1 for line in lines if "," in line)
    print(f"【阶段3-输出完成】最终有效流媒体总条数：{stream_count}", flush=True)

def main():
    print("====== IPTV分拣脚本启动 ======", flush=True)
    source_pool = load_source_list()
    white_origin_list, white_lower_set = load_white_list()
    raw_channel_cache = defaultdict(list)
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_FETCH_WORKERS) as exe:
        futures = [exe.submit(fetch_channel_from_source, s, white_lower_set) for s in source_pool]
        for fu in futures:
            try:
                pair_list = fu.result(timeout=SOURCE_FETCH_TIMEOUT + 2)
                for ch, link in pair_list:
                    raw_channel_cache[ch].append(link)
            except concurrent.futures.TimeoutError:
                print("【警告】单个直播源拉取超时，自动跳过", flush=True)
            except Exception as e:
                print(f"【警告】直播源处理异常：{str(e)}", flush=True)
    # 频道链接去重
    for ch in raw_channel_cache:
        raw_channel_cache[ch] = list(dict.fromkeys(raw_channel_cache[ch]))
    print(f"【阶段1完成】待测速频道总数量：{len(raw_channel_cache)}", flush=True)
    qualified_channel_map = filter_best_streams(raw_channel_cache)
    print(f"【阶段2完成】完成测速筛选频道数量：{len(qualified_channel_map)}", flush=True)
    export_result(white_origin_list, qualified_channel_map)

    # 新增核心根治代码：强制关闭所有http连接池，等待所有网络线程退出
    import urllib3
    urllib3.PoolManager().clear()
    urllib3.disable_warnings()
    # 等待2秒，让底层IO线程全部销毁
    time.sleep(2)

    # 原收尾打印
    urllib3.PoolManager().clear()
    os.sync()
    time.sleep(0.5)
    print("====== Python资源全部释放完成 ======", flush=True)

if __name__ == "__main__":
    main()
