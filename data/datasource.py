import time
import random
import akshare as ak
import efinance as ef
from config.settings import MIN_SLEEP, MAX_SLEEP, RETRY_TIMES

def random_sleep():
    time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

def safe_request(func, *args, **kwargs):
    for i in range(RETRY_TIMES):
        try:
            res = func(*args, **kwargs)
            random_sleep()
            return res
        except Exception as e:
            print(f"请求失败，重试{i+1}/{RETRY_TIMES} 错误：{str(e)[:80]}")
            time.sleep(1.2)
    return None

def get_cailian_news():
    return safe_request(ak.stock_info_cailian)

def get_sector_fund():
    return safe_request(ak.stock_sector_fund_flow_rank)

def get_sector_stock(sector_name):
    df = safe_request(ak.stock_board_industry_name_em)
    if df is None:
        return None
    return df[df["板块名称"] == sector_name].copy()

def get_stock_kline(code):
    return safe_request(ef.get_quote_history, code)

def get_stock_north_money(code):
    try:
        df = safe_request(ak.stock_hsgt_individual_em, symbol=code)
        if df is None or df.empty:
            return 0
        df = df.tail(10)
        return df["北向资金净流入-万"].sum()
    except:
        return 0

def get_stock_limit_up(code):
    df = safe_request(ak.stock_zt_pool_em)
    if df is None or df.empty:
        return 0
    cnt = len(df[df["代码"] == str(code)])
    return cnt

def get_stock_announcement(code):
    return safe_request(ak.stock_info_cninfo, symbol=code)
