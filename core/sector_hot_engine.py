import jieba
import pandas as pd
from data.datasource import get_cailian_news, get_sector_fund
from config.settings import HOT_SCORE_THRESHOLD
from core.market_sentiment import calc_sentiment_score


def analyze_hot_sector():
    fund_df = get_sector_fund()
    source_type = "东方财富资金接口"

    # 东方财富接口失效：使用内置静态行业板块列表，不再访问任何外网接口
    if fund_df is None or fund_df.empty:
        print("⚠️东方财富板块资金接口访问失败，启用【内置静态板块列表】，资金权重归零")
        source_type = "内置静态板块"
        static_sector_list = [
            {"板块名称": "半导体", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "人工智能", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "储能", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "光伏", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "锂电池", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "军工", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "医药", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "房地产", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "券商", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "汽车整车", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "机器人", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "算力", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "传媒", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "煤炭", "主力净流入-亿": 0, "涨跌幅": 0},
            {"板块名称": "有色", "主力净流入-亿": 0, "涨跌幅": 0},
        ]
        fund_df = pd.DataFrame(static_sector_list)

    # ==========修复新闻代码，增加列存在判断，杜绝KeyError==========
    news_df = get_cailian_news()
    word_freq = {}
    if news_df is not None and not news_df.empty and "标题" in news_df.columns:
        all_title = "".join(news_df["标题"].astype(str))
        words = jieba.lcut(all_title)
        for w in words:
            if len(w) >= 2:
                word_freq[w] = word_freq.get(w, 0) + 1
    else:
        print("ℹ️新闻接口不可用或无【标题】字段，新闻热度权重清零")

    sentiment_info = calc_sentiment_score()
    emotion_factor = sentiment_info["factor"]
    hot_result = []

    for _, row in fund_df.iterrows():
        sector_name = row["板块名称"]
        try:
            fund_flow = float(row["主力净流入-亿"])
        except:
            fund_flow = 0
        try:
            sector_chg = float(row["涨跌幅"])
        except:
            sector_chg = 0

        news_score = word_freq.get(sector_name, 0) * 0.3
        fund_score = max(fund_flow, 0) * 0.4
        change_score = max(sector_chg, 0) * 30 * 0.3
        total_score = news_score + fund_score + change_score
        total_score = round(total_score * emotion_factor, 2)

        if total_score >= HOT_SCORE_THRESHOLD:
            hot_result.append({
                "sector_name": sector_name,
                "total_score": total_score,
                "main_fund_flow": fund_flow,
                "sector_change": sector_chg
            })

    hot_sector_df = pd.DataFrame(hot_result)
    if not hot_sector_df.empty:
        hot_sector_df = hot_sector_df.sort_values("total_score", ascending=False)
        print(f"✅ 数据源：{source_type}，筛选达标板块数量：{len(hot_sector_df)}")
    else:
        print(f"⚠️数据源：{source_type}，无板块达到热度阈值")

    return hot_sector_df, sentiment_info
