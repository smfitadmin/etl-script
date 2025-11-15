#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
อ่าน Excel → JSON พร้อมแปลงวันที่แบบถูกต้อง 100%:
- ทุกฟิลด์ที่เป็นวันที่จริง ใช้ dayfirst=True เสมอ
- output รูปแบบ YYYY-MM-DD
"""

import json
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from pandas import Timestamp


# -------------------------
# list column ที่ถือว่าเป็นวันที่
# (เพิ่มได้ตามไฟล์จริง)
# -------------------------
DATE_COLUMNS = [
    "วันที่",
    "วันที่จ่ายเงิน",
    "remittance_date",
    "sent_date",
    "sent_time",
    "pay_date",
    "doc_date",
    "payment_date",
]


def is_date_column(col_name: str) -> bool:
    if col_name is None:
        return False
    return str(col_name).strip().lower() in [c.lower() for c in DATE_COLUMNS]


# -------------------------
# ฟังก์ชันแปลงวันที่เป็น YYYY-MM-DD
# ใช้ dayfirst=True เสมอ
# -------------------------
def parse_date(value):
    if value is None:
        return None

    # datetime หรือ Timestamp
    if isinstance(value, (Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")

    # excel serial number
    if isinstance(value, (int, float)):
        try:
            dt = pd.to_datetime(value, unit="d", origin="1899-12-30")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return value

    # string
    if isinstance(value, str):
        txt = value.strip()
        if txt == "":
            return None

        # ถ้าเป็น 2025-10-30 อยู่แล้ว
        if len(txt) == 10 and txt[4] == "-" and txt[7] == "-":
            return txt

        # parse DD/MM/YYYY
        try:
            dt = pd.to_datetime(txt, dayfirst=True)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return txt

    return value


def excel_to_json(excel_path: str):
    excel_path = Path(excel_path)

    if not excel_path.exists():
        raise FileNotFoundError(f"ไม่พบไฟล์: {excel_path}")

    supplier_code = excel_path.stem

    out_dir = Path("processed_data/rm")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{supplier_code}.json"

    xls = pd.read_excel(excel_path, sheet_name=None)

    output = {
        "file_name": excel_path.name,
        "supplier_code": supplier_code,
        "sheets": []
    }

    for sheet_name, df in xls.items():
        df = df.where(pd.notnull(df), None)

        rows = df.to_dict(orient="records")
        updated = []

        for r in rows:
            new_r = {}

            for col, val in r.items():
                if is_date_column(col):
                    new_r[col] = parse_date(val)
                else:
                    new_r[col] = val

            new_r["supplier_code"] = supplier_code
            updated.append(new_r)

        output["sheets"].append({
            "sheet_name": sheet_name,
            "rows": updated
        })

    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"✓ Completed → {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("วิธีใช้:")
        print('  python read_rem_detail_to_json.py "raw_data/rm/72195.xlsx"')
        sys.exit(1)

    excel_to_json(sys.argv[1])
