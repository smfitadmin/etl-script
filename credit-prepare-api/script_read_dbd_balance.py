#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
script_read_dbd_balance.py

Usage:
  python script_read_dbd_balance.py \
    --folder ./downloads \
    --outdir ./out_json \
    [--sheet SHEETNAME] [--debug]

อัปเดต:
- รองรับ .xls และ .xlsx
- ถ้าเจอ '-' หรือ '0' → 0.0
- ถ้า amount และ pct_change เป็น None → 0.0
"""

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import warnings
import pandas as pd

# --------------------- mapping: TH -> EN (ชื่อรายการ) --------------------- #
TH_TO_EN_MAP = {
    "ลูกหนี้การค้าสุทธิ": "accounts_receivable_net",
    "ลูกหนี้การค้า": "accounts_receivable",
    "สินค้าคงเหลือ": "inventories",
    "สินทรัพย์หมุนเวียน": "current_assets",
    "ที่ดิน อาคารและอุปกรณ์": "property_plant_equipment",
    "สินทรัพย์ไม่หมุนเวียน": "non_current_assets",
    "สินทรัพย์รวม": "total_assets",
    "หนี้สินหมุนเวียน": "current_liabilities",
    "หนี้สินไม่หมุนเวียน": "non_current_liabilities",
    "หนี้สินรวม": "total_liabilities",
    "ส่วนของผู้ถือหุ้น": "shareholders_equity",
    "หนี้สินรวมและส่วนของผู้ถือหุ้น": "total_liabilities_and_shareholder_equity",
}
IGNORE_ROW_TOKENS = {None, "", "nan", "หน่วย : บาท", "%เปลี่ยนแปลง", "จำนวนเงิน"}

# --------------------- helpers --------------------- #
def log(debug: bool, *args):
    if debug:
        print(*args)

def normalize_th(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return s.replace("\u200b", "").replace("\xa0", " ").strip()

def coerce_numeric(x: Any) -> Optional[float]:
    """
    แปลงข้อความเป็น float
    - '-' หรือ '0' → 0.0
    - '(123)' → -123.0
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip().replace(",", "")
    if s in ("", "-", "–", "—", "0", "0.0"):
        return 0.0

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]

    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        # ถ้าแปลงไม่ได้ ให้เป็น 0.0
        return 0.0

def to_gregorian(y: int) -> Optional[int]:
    if y is None:
        return None
    if 2400 <= y <= 2600:
        y -= 543
    if 1900 <= y <= 2100:
        return y
    return None

def parse_year_like(s: Any) -> Optional[int]:
    if s is None:
        return None
    m = re.search(r"(\d{4})", str(s))
    return to_gregorian(int(m.group(1))) if m else None

# --------------------- sniff & read --------------------- #
def magic_bytes(path: Path, n: int = 8) -> bytes:
    with open(path, "rb") as f:
        return f.read(n)

def sniff_format(path: Path) -> str:
    sig = magic_bytes(path, 8)
    if sig.startswith(b"PK\x03\x04"):
        return "xlsx"
    if sig.startswith(b"\xD0\xCF\x11\xE0"):
        return "xls"
    return "unknown"

def read_table(path: Path, sheet: Optional[str], debug: bool) -> pd.DataFrame:
    def _ensure_df(obj) -> pd.DataFrame:
        if isinstance(obj, dict):
            return next(iter(obj.values()))
        return obj

    kind = sniff_format(path)
    print(f"  ↪ detected format: {kind}")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sheet_name = 0 if (sheet is None or str(sheet).strip() == "") else sheet

        # ลำดับ engine ที่พยายามใช้
        engines = []
        if kind == "xls":
            engines = ["xlrd"]  # ต้องติดตั้ง xlrd==1.2.0
        elif kind == "xlsx":
            engines = ["openpyxl"]
        else:
            # ไม่แน่ใจ ลองทั้งสอง
            engines = ["openpyxl", "xlrd"]

        last_err = None
        for eng in engines:
            try:
                df = pd.read_excel(path, sheet_name=sheet_name, engine=eng, header=None)
                return _ensure_df(df)
            except Exception as e:
                last_err = e
                log(debug, f"read_excel with engine={eng} failed: {e}")

        # ทางหนีไฟ: ถ้าเป็นไฟล์ xls ที่จริงเป็น html-Excel ให้ลอง read_html
        try:
            tables = pd.read_html(path, header=None)
            if tables:
                return tables[0]
        except Exception as e2:
            last_err = e2

        raise RuntimeError(f"อ่านไฟล์ไม่สำเร็จ: {path.name} (last error: {last_err})")

# --------------------- tidy --------------------- #
def find_header_row(df: pd.DataFrame, debug: bool) -> int:
    best_row, best_cnt = 0, -1
    for i in range(min(30, len(df))):
        row = df.iloc[i].tolist()
        cnt = sum(1 for v in row if parse_year_like(v) is not None)
        if cnt > best_cnt:
            best_cnt, best_row = cnt, i
    log(debug, f"candidate header row index: {best_row}")
    return best_row

def tidy_after_header(df_raw: pd.DataFrame, header_idx: int, debug: bool) -> pd.DataFrame:
    header = df_raw.iloc[header_idx].tolist()
    data = df_raw.iloc[header_idx + 1:].copy()
    data.columns = [str(c) if c is not None else "nan" for c in header]
    data = data.dropna(how="all").dropna(axis=1, how="all").reset_index(drop=True)
    data.columns = [normalize_th(c) or "nan" for c in data.columns]
    return data

def detect_year_pairs(df: pd.DataFrame, debug: bool) -> List[Tuple[int, str, Optional[str]]]:
    cols = list(df.columns)
    pairs: List[Tuple[int, str, Optional[str]]] = []
    i = 1
    while i < len(cols):
        y = parse_year_like(cols[i])
        if y is not None:
            val_col = cols[i]
            pct_col = None
            if i + 1 < len(cols):
                nxt = normalize_th(cols[i + 1]).lower()
                if nxt in ("", "nan") or "%" in nxt or "เปลี่ยนแปลง" in nxt or "เปลี่ยน" in nxt:
                    pct_col = cols[i + 1]
                    i += 2
                else:
                    i += 1
            else:
                i += 1
            pairs.append((y, val_col, pct_col))
        else:
            i += 1
    pairs = [(y, v, p) for (y, v, p) in pairs if 1900 <= y <= 2100]
    log(debug, f"year pairs: {[(y, v, p) for (y, v, p) in pairs]}")
    return pairs

# --------------------- item_en helper --------------------- #
def get_item_en(th_name: str) -> str:
    name = normalize_th(th_name)
    if name in TH_TO_EN_MAP:
        return TH_TO_EN_MAP[name]
    # fallback แบบยืดหยุ่น
    if "หนี้สินไม่หมุนเวียน" in name:
        return "non_current_liabilities"
    if "ลูกหนี้การค้า" in name:
        return "trade_receivables"
    if "สินค้าคงเหลือ" in name:
        return "inventories"
    if "สินทรัพย์หมุนเวียน" in name:
        return "current_assets"
    if "สินทรัพย์ไม่หมุนเวียน" in name:
        return "non_current_assets"
    if "สินทรัพย์รวม" in name:
        return "total_assets"
    if "หนี้สินหมุนเวียน" in name:
        return "current_liabilities"
    if "หนี้สินรวมและส่วนของผู้ถือหุ้น" in name:
        return "total_equity_and_liabilities"
    if "หนี้สินรวม" in name:
        return "total_liabilities"
    if "ผู้ถือหุ้น" in name:
        return "shareholders_equity"
    return "unknown"

# --------------------- main transform --------------------- #
def dataframe_to_year_json(df: pd.DataFrame, tax_id: str, debug: bool) -> Dict[str, List[Dict[str, Any]]]:
    label_col = df.columns[0]
    work = df.copy()
    work[label_col] = work[label_col].map(normalize_th)
    work = work[~work[label_col].isin(IGNORE_ROW_TOKENS)].reset_index(drop=True)

    pairs = detect_year_pairs(work, debug)
    if not pairs:
        raise RuntimeError("ไม่พบคอลัมน์ปีใน header")

    by_year: Dict[str, List[Dict[str, Any]]] = {}
    for y, val_col, pct_col in pairs:
        year_key = str(y)
        rows: List[Dict[str, Any]] = []
        for _, row in work.iterrows():
            th_item = normalize_th(row.get(label_col))
            if th_item in IGNORE_ROW_TOKENS:
                continue

            amount = coerce_numeric(row.get(val_col))
            pct = coerce_numeric(row.get(pct_col)) if pct_col else 0.0

            # บังคับ default 0.0 ตามเงื่อนไข
            if amount is None:
                amount = 0.0
            if pct is None:
                pct = 0.0

            rec = {
                "item": th_item,
                "item_en": get_item_en(th_item),
                "amount": float(amount),
                "pct_change": float(pct),
                "tax_id": tax_id,
            }
            rows.append(rec)

        if rows:
            by_year.setdefault(year_key, []).extend(rows)

    return by_year

# --------------------- per-file processing --------------------- #
def extract_tax_id_from_name(path: Path) -> Optional[str]:
    m = re.match(r"^(\d+)_balance\.xls[x]?$", path.name, re.IGNORECASE)
    return m.group(1) if m else None

def process_one_file(path: Path, outdir: Path, sheet: Optional[str], debug: bool):
    tax_id = extract_tax_id_from_name(path) or ""
    print(f"\n▶ Processing: {path.name} (tax_id={tax_id})")

    df_raw = read_table(path, sheet, debug)
    hdr = find_header_row(df_raw, debug)
    df = tidy_after_header(df_raw, hdr, debug)
    data = dataframe_to_year_json(df, tax_id, debug)

    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"{tax_id}_balance.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✔ wrote: {outfile.resolve()} (years={list(data.keys())})")

def process_folder(folder: Path, outdir: Path, sheet: Optional[str], debug: bool):
    files = sorted(list(folder.glob("*_balance.xls")) + list(folder.glob("*_balance.xlsx")))
    if not files:
        print("ไม่พบไฟล์ *_balance.xls หรือ *_balance.xlsx ในโฟลเดอร์ที่กำหนด")
        return
    for p in files:
        process_one_file(p, outdir, sheet, debug)

# --------------------- CLI --------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    process_folder(Path(args.folder), Path(args.outdir), args.sheet, args.debug)

if __name__ == "__main__":
    main()
