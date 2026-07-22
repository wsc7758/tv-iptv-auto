from config.constants import MOAT_INDUSTRY, MOAT_KEY_WORDS, WEAK_MOAT_WORD
from data.datasource import safe_request
import akshare as ak

def get_stock_business_info(code: str):
    try:
        df = safe_request(ak.stock_individual_info_em, symbol=code)
        if df is None or df.empty:
            return ""
        business_series = df.loc[df["item"] == "主营业务", "value"]
        if len(business_series) > 0:
            return str(business_series.values[0])
        return ""
    except Exception as e:
        print(f"{code} 获取主营业务失败 {e}")
        return ""

def get_stock_industry(code: str):
    try:
        df = safe_request(ak.stock_board_concept_name_em)
        if df is None:
            return []
        res = df[df["股票代码"] == code]["板块名称"].unique().tolist()
        return res
    except:
        return []

def detect_moat(code: str, stock_name: str) -> dict:
    business_text = get_stock_business_info(code)
    industry_list = get_stock_industry(code)
    evidence_list = []

    for ind in industry_list:
        for moat_ind in MOAT_INDUSTRY:
            if moat_ind in ind:
                evidence_list.append(f"所属赛道【{ind}】属于高护城河赛道")

    hit_keys = []
    for kw in MOAT_KEY_WORDS:
        if kw in business_text:
            hit_keys.append(kw)
    if hit_keys:
        evidence_list.append(f"主营业务匹配壁垒关键词：{','.join(hit_keys)}")

    weak_flag = False
    for wkw in WEAK_MOAT_WORD:
        if wkw in business_text:
            weak_flag = True
            break

    has_moat = False
    tip = "无明显护城河特征，建议谨慎研究基本面"
    evidence = "暂无支撑依据"

    if len(evidence_list) >= 1 and not weak_flag:
        has_moat = True
        tip = "✅【重点提示：该标的初步识别具备护城河属性，请进一步深度调研财报验证！】"
        evidence = "；".join(evidence_list)
    elif weak_flag and len(evidence_list)>=1:
        tip = "⚠️赛道具备潜在壁垒，但业务描述存在竞争压力提示，护城河存疑，务必仔细甄别！"
        evidence = "；".join(evidence_list) + "；同时检测到竞争加剧相关描述"

    return {
        "has_moat": has_moat,
        "moat_tip": tip,
        "evidence": evidence
    }
