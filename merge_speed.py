import requests
import concurrent.futures
import re
from collections import defaultdict
import time

# 配置项
SOURCE_FILE = "sources.txt"
OUTPUT_TXT = "tv.txt"
TEST_TIMEOUT = 3  # 测速超时3秒，超时判定失效源
MAX_BEST_PER_CHANNEL = 3  # 每个频道保留前3最快线路

# 读取所有直播源链接
def load_source_urls():
    with open(SOURCE_FILE, "r", encoding="utf-8") as f:
        lines = [i.strip() for i in f.readlines() if i.strip()]
    return lines

# 拉取单个txt/m3u直播源，提取 频道名,url
def fetch_iptv(url):
    channels = []
    try:
        headers = {"User-Agent":"Mozilla/5.0 Android TV"}
        resp = requests.get(url, headers=headers, timeout=5)
        resp.encoding = resp.apparent_encoding
        text = resp.text
        # 匹配标准 txt 格式：频道名,链接
        txt_pattern = re.compile(r"([^,]+),(http[^\n]+)")
        txt_matches = txt_pattern.findall(text)
        for name, link in txt_matches:
            name = name.strip().replace("#genre#","").strip()
            if name and link:
                channels.append((name, link.strip()))
        # 兼容m3u格式 #EXTINF:-1,频道\nurl
        m3u_pattern = re.compile(r"#EXTINF:-1,([^\n]+)\n(http[^\n]+)")
        m3u_matches = m3u_pattern.findall(text)
        for name, link in m3u_matches:
            name = name.strip()
            if name and link:
                channels.append((name, link.strip()))
    except Exception as e:
        pass
    return channels

# 测速单个播放链接，返回延迟，超时返回9999
def test_speed(url):
    start = time.time()
    try:
        headers = {"User-Agent":"Mozilla/5.0 Android TV"}
        r = requests.head(url, headers=headers, timeout=TEST_TIMEOUT, allow_redirects=True)
        cost = time.time() - start
        return round(cost,3)
    except:
        return 9999

def main():
    all_channels = []
    source_urls = load_source_urls()
    # 多线程拉取所有源
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        res_list = executor.map(fetch_iptv, source_urls)
        for res in res_list:
            all_channels.extend(res)
    # 按频道分组 {频道名:[url列表]}
    group = defaultdict(list)
    for name,url in all_channels:
        group[name].append(url)
    # 对每个频道的所有线路测速排序
    final = []
    for ch_name, urls in group.items():
        test_map = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            speed_res = executor.map(test_speed, urls)
            for u,s in zip(urls, speed_res):
                test_map[u] = s
        # 按延迟从小到大排序，过滤超时源
        sorted_urls = sorted([u for u in test_map if test_map[u] < TEST_TIMEOUT], key=lambda x:test_map[x])
        # 保留最优N条
        top_urls = sorted_urls[:MAX_BEST_PER_CHANNEL]
        for u in top_urls:
            final.append(f"{ch_name},{u}")
    # 写入输出tv.txt
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(final))
    print(f"聚合完成，共生成 {len(final)} 条有效线路，输出{OUTPUT_TXT}")

if __name__ == "__main__":
    main()