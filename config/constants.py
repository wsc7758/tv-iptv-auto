import os
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 文件夹路径
RUNTIME_LOG = os.path.join(BASE_PATH, "runtime", "logs")
RUNTIME_CACHE = os.path.join(BASE_PATH, "runtime", "cache")
OUTPUT_LATEST = os.path.join(BASE_PATH, "output", "latest")
OUTPUT_ARCHIVE = os.path.join(BASE_PATH, "output", "archive")

# 输出文件名
JSON_RESULT_NAME = "stock_target.json"
MD_RESULT_NAME = "hot_sector.md"

# 护城河关键词库
MOAT_INDUSTRY = {
    "白酒", "调味品", "创新药", "医疗器械", "稀缺矿产资源",
    "高端芯片制造", "工业软件", "云计算龙头", "机场高速",
    "水电", "品牌消费龙头", "独家牌照金融", "稀土资源",
    "海上风电核心零部件", "储能核心材料龙头"
}
MOAT_KEY_WORDS = [
    "龙头", "全球领先", "国内唯一", "独家", "寡头", "专利壁垒",
    "资源稀缺", "不可再生", "特许经营", "牌照", "品牌壁垒",
    "规模优势", "成本优势", "核心自主知识产权"
]
WEAK_MOAT_WORD = [
    "竞争加剧", "产能过剩", "价格战", "无核心技术", "同质化严重"
]

# 全局免责文本
DISCLAIMER_TEXT = """
# ⚠️ 重要风险免责
本项目仅用于Python金融数据分析编程学习，所有资讯、指标、支撑压力、交易策略
**全部仅为历史数据推演研究参考，绝对不构成任何投资建议！**
股市存在极高风险，请勿直接依据程序输出进行实盘操作。
禁止对外分发选股名单、交易策略。
"""
