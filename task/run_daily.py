"""
GitHub Actions 每日任务入口
"""
from datetime import date
import chinese_calendar
from exporter.md_exporter import export_md
from exporter.json_exporter import export_json
from core.sector_hot_engine import analyze_hot_sector
from core.stock_rank_engine import get_sector_leaders

DISCLAIMER = """
【重要免责声明】
本程序仅用于Python金融数据分析编程学习，所有资讯、指标、支撑压力、交易策略仅作个人复盘参考！
不构成任何投资建议，股市存在巨大风险，请勿依据程序结果实盘交易。
禁止对外分发选股清单、交易策略。
"""

if __name__ == "__main__":
    print(DISCLAIMER)
    # 判断A股交易日，休市直接退出
    if not chinese_calendar.is_workday(date.today()):
        print("当前非A股交易日，程序结束。")
        exit(0)

    print("开始执行：热点板块分析")
    hot_sector_df, sentiment_info = analyze_hot_sector()
    if hot_sector_df.empty:
        print("未筛选出达标热点板块，任务结束")
        exit()

    final_result = []
    for _, sec_row in hot_sector_df.iterrows():
        sec_name = sec_row["sector_name"]
        print(f"正在处理板块：{sec_name}")
        stock_df = get_sector_stock(sec_name)
        if stock_df is None or stock_df.empty:
            continue
        leader_list = get_sector_leaders(stock_df, sentiment_info)
        final_result.append({
            "sector_name": sec_name,
            "total_score": sec_row["total_score"],
            "leaders": leader_list
        })

    # 导出文件
    export_md(final_result, sentiment_info)
    export_json(final_result)
    print("✅ 选股任务执行完成！")
