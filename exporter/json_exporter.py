import json
import os
from datetime import datetime
from config.constants import OUTPUT_LATEST, OUTPUT_ARCHIVE, JSON_RESULT_NAME

def init_folder():
    os.makedirs(OUTPUT_LATEST, exist_ok=True)
    os.makedirs(OUTPUT_ARCHIVE, exist_ok=True)

def export_json(final_result):
    init_folder()
    today = datetime.now().strftime("%Y-%m-%d")
    out_path = os.path.join(OUTPUT_LATEST, JSON_RESULT_NAME)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)
    # 归档快照
    arch_path = os.path.join(OUTPUT_ARCHIVE, f"{today}_{JSON_RESULT_NAME}")
    with open(arch_path, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)
