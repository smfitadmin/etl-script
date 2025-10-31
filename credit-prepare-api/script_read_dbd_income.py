#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
script_read_dbd_income.py — robust reader for DBD *_income.xls/xlsx → JSON (per year)

คุณสมบัติ
- คง "รายการ" (item_th) และลำดับตามไฟล์จริง (orig_index)
- map item_en ตาม TH_TO_EN_INCOME (ยืดหยุ่นเรื่องวงเล็บ/ช่องว่าง) ไม่เจอแมป → "unknown"
- รองรับ .xls/.xlsx และกรณี .xlsx ที่จริงเป็น .xls (BadZipFile)
- ลองอ่านตามชนิดไฟล์: xlsx→openpyxl ; xls→xlrd(<2.0)→calamine ; ท้ายสุด read_html
- ค่า '-', '–', '—', '0', '0.0' หรือค่าเลขที่เป็นศูนย์ → 0.0 เสมอ (และจะถูกเขียนลง JSON)
- JSON รูปแบบ: { "<year>": [ { item, item_en, amount, pct_change, tax_id }, ... ] }

วิธีใช้:
  pip install "xlrd==1.2.0" pandas openpyxl pandas-calamine
  python script_read_dbd_income.py --folder ./downloads --outdir ./out_json --debug
"""

from __future__ import annotations
import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# ---------------- Utils ---------------- #

def log(debug: bool, *args):
    if debug:
        print(*args)

def is_none_or_nan(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return False

YEAR_CELL_RE = re.compile(r"^\s*(\d{4})\s*$")

def to_gregorian_year(val: Any) -> Optional[int]:
    """
    รับปี พ.ศ./ค.ศ. คืนปี ค.ศ. (int) ถ้าไม่ใช่ปี -> None
    """
    if is_none_or_nan(val):
        return None
    s = str(val).strip()
    m = YEAR_CELL_RE.match(s)
    if not m:
        try:
            y = int(float(s))
        except Exception:
            return None
    else:
        y = int(m.group(1))
    if y >= 2400:
        y -= 543
    if 1900 <= y <= 2100:
        return y
    return None

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

# จับตัวเลขที่อาจมีคอมมา/ช่องว่าง
_NUM_RE = re.compile(r"[-+]?\d+(?:[,\s]\d{3})*(?:\.\d+)?")

def _is_dash_or_zero_str(s: str) -> bool:
    """ถือว่าเป็นศูนย์: '-', '–', '—', '0', '0.0' (+/-0 ก็ถือเป็นศูนย์)"""
    s2 = normalize_spaces(s).replace(",", "")
    return s2 in {"-", "–", "—", "0", "+0", "-0", "0.0", "+0.0", "-0.0"}

def to_float_or_zero(x: Any) -> Optional[float]:
    """
    แปลงค่าเป็น float:
    - '-', '–', '—', '0', '0.0' หรือเลขศูนย์ => 0.0
    - ตัวเลขทั่วไป (รองรับคอมมา/ช่องว่าง) => float
    - อื่น ๆ ที่แปลงไม่ได้ => None
    """
    if is_none_or_nan(x):
        return None
    if isinstance(x, (int, float)):
        return 0.0 if float(x) == 0.0 else float(x)
    s = str(x)
    if _is_dash_or_zero_str(s):
        return 0.0
    m = _NUM_RE.search(s.replace(" ", ""))
    if not m:
        return None
    token = m.group(0).replace(",", "")
    try:
        val = float(token)
        return 0.0 if val == 0.0 else val
    except Exception:
        return None


# ---------------- Thai->English mapping (income only) ---------------- #

TH_TO_EN_INCOME: Dict[str, str] = {
    # core set
    "รายได้หลัก": "net_revenue",
    "รายได้รวม": "total_revenue",
    "ต้นทุนขาย": "cost_of_goods_sold",
    "ค่าใช้จ่ายในการขายและบริหาร": "operating_expenses",
    "รายจ่ายรวม": "total_expenses",
    "ดอกเบี้ยจ่าย": "interest_expenses",
    "ภาษีเงินได้": "income_tax_expenses",

    # กำไร(ขาดทุน) variants (canonical keys)
    "กำไร(ขาดทุน) ขั้นต้น": "gross_profit",
    "กำไร(ขาดทุน) ก่อนภาษี": "income_before_tax",
    "กำไร(ขาดทุน) สุทธิ": "net_income",

    # no-paren fallbacks (เผื่อ OCR/รูปแบบ)
    "กำไรขาดทุน ขั้นต้น": "gross_profit",
    "กำไรขาดทุน ก่อนภาษี": "income_before_tax",
    "กำไรขาดทุน สุทธิ": "net_income",
}

# ตัวช่วย normalize พิเศษ (ตัด zero-width, NBSP, เว้นวรรคเกิน, วงเล็บแปลก)
_ZW_RE = re.compile(r"[\u200b\u200c\u200d\u2060]")
def _canon_title(s: str) -> str:
    s = _ZW_RE.sub("", s)
    s = s.replace("\xa0", " ")              # NBSP → space
    s = s.replace("（", "(").replace("）", ")")
    # มาตรฐานรูปแบบวงเล็บ "กำไร(ขาดทุน)" และตัดช่องว่างรอบวงเล็บ
    s = re.sub(r"\s*\(\s*", "(", s)
    s = re.sub(r"\s*\)\s*", ")", s)
    # กรณีเขียนแบบไม่มีวงเล็บ: "กำไร ขาดทุน" → "กำไร(ขาดทุน)"
    s = re.sub(r"กำไร\s*ขาดทุน", "กำไร(ขาดทุน)", s)
    # บีบช่องว่างซ้ำ
    s = re.sub(r"\s+", " ", s).strip()
    return s

def map_item_th_to_en(th_name: Any) -> str:
    if is_none_or_nan(th_name):
        return "unknown"

    name = _canon_title(str(th_name))

    # 1) ลองตรง ๆ ก่อน
    if name in TH_TO_EN_INCOME:
        return TH_TO_EN_INCOME[name]

    # 2) ลบวงเล็บทั้งหมดเป็น fallback
    name_no_paren = re.sub(r"[()（）\[\]{}]", "", name).strip()
    if name_no_paren in TH_TO_EN_INCOME:
        return TH_TO_EN_INCOME[name_no_paren]

    # 3) regex fallback สำหรับกลุ่ม "กำไร(ขาดทุน) ..."
    #    - ขั้นต้น → gross_profit
    #    - ก่อนภาษี → profit_before_tax
    #    - สุทธิ → net_profit
    if name.startswith("กำไร(ขาดทุน)"):
        if "ขั้นต้น" in name:
            return "gross_profit"
        if "ก่อนภาษี" in name:
            return "profit_before_tax"
        if "สุทธิ" in name:
            return "net_profit"

    return "unknown"


# ---------------- Sniff & Readers ---------------- #

def magic_bytes(path: Path, n: int = 8) -> bytes:
    with open(path, "rb") as f:
        return f.read(n)

def sniff_kind(path: Path) -> str:
    sig = magic_bytes(path, 8)
    if sig.startswith(b"PK\x03\x04"):
        return "xlsx"
    if sig.startswith(b"\xD0\xCF\x11\xE0"):
        return "xls"
    # ถ้า magic bytes ไม่ชัดเจน ใช้นามสกุลช่วยตัดสิน
    ext = path.suffix.lower()
    if ext == ".xlsx":
        return "xlsx"
    if ext == ".xls":
        return "xls"
    return "unknown"

def _read_openpyxl(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl", header=None)

def _read_xlrd(path: Path) -> pd.DataFrame:
    # ต้อง xlrd<2.0 สำหรับ .xls
    return pd.read_excel(path, engine="xlrd", header=None)

def _read_calamine(path: Path) -> pd.DataFrame:
    # ต้องติดตั้ง pandas-calamine
    return pd.read_excel(path, engine="calamine", header=None)

def read_income_table(path: Path, debug: bool) -> pd.DataFrame:
    """
    อ่านไฟล์ Excel income ให้ได้ DataFrame ดิบ (ไม่ tidy)
    ลำดับความพยายาม:
      - ถ้า sniff เป็น xlsx: openpyxl → (เผื่อไฟล์จริงเป็น xls) xlrd → calamine → read_html
      - ถ้า sniff เป็น xls/unknown: xlrd → calamine → (เผื่อจริงเป็น xlsx) openpyxl → read_html
    """
    kind = sniff_kind(path)
    log(debug, f"  ↪ sniffed kind: {kind} for {path.name}")
    tried = []

    if kind == "xlsx":
        try:
            df = _read_openpyxl(path)
            log(debug, f"  ✔ read with openpyxl: shape={df.shape}")
            return df
        except Exception as e:
            tried.append(("openpyxl", str(e)))
            log(debug, f"  ⚠ openpyxl failed (maybe actually .xls): {e}")
        for eng_name, reader in (("xlrd", _read_xlrd), ("calamine", _read_calamine)):
            try:
                df = reader(path)
                log(debug, f"  ✔ read with {eng_name}: shape={df.shape}")
                return df
            except Exception as e:
                tried.append((eng_name, str(e)))
                log(debug, f"  ⚠ failed read with {eng_name}: {e}")
    else:
        # xls หรือ unknown
        for eng_name, reader in (("xlrd", _read_xlrd), ("calamine", _read_calamine)):
            try:
                df = reader(path)
                log(debug, f"  ✔ read with {eng_name}: shape={df.shape}")
                return df
            except Exception as e:
                tried.append((eng_name, str(e)))
                log(debug, f"  ⚠ failed read with {eng_name}: {e}")
        # เผื่อเป็น xlsx จริง
        try:
            df = _read_openpyxl(path)
            log(debug, f"  ✔ read with openpyxl (fallback): shape={df.shape}")
            return df
        except Exception as e:
            tried.append(("openpyxl", str(e)))
            log(debug, f"  ⚠ openpyxl fallback failed: {e}")

    # ทางหนีไฟ: html/ตารางฝัง
    try:
        tables = pd.read_html(path, header=None)
        if tables:
            df = tables[0]
            log(debug, f"  ✔ read with read_html: shape={df.shape}")
            return df
    except Exception as e:
        tried.append(("read_html", str(e)))
        log(debug, f"  ⚠ failed read_html: {e}")

    raise RuntimeError(f"Cannot read Excel file: {path.name}; tried={tried}")


# ---------------- Tidy ---------------- #

def detect_header_row(df: pd.DataFrame, debug: bool) -> int:
    """
    หาแถวหัวตารางจากจำนวน cell ที่เป็น 'ปี' มากที่สุด (รองรับ พ.ศ./ค.ศ.)
    """
    best_idx, best_cnt = 0, -1
    for i in range(min(len(df), 40)):
        row = df.iloc[i]
        cnt = sum(1 for v in row if to_gregorian_year(v) is not None)
        if cnt > best_cnt:
            best_cnt, best_idx = cnt, i
    log(debug, f"  ↪ header candidate row: {best_idx} (year-like cells={best_cnt})")
    return best_idx

def tidy_income_table(df_raw: pd.DataFrame, debug: bool) -> pd.DataFrame:
    """
    คืน DataFrame columns = ['item_th', year1, year2, ...] และเพิ่ม 'orig_index' เพื่อคงลำดับตามไฟล์
    """
    if df_raw.empty:
        return pd.DataFrame(columns=["item_th", "orig_index"])

    hdr = detect_header_row(df_raw, debug)
    header_row = df_raw.iloc[hdr].tolist()

    year_cols: List[int] = []
    year_names: List[str] = []
    first_year_col_idx: Optional[int] = None

    for j, val in enumerate(header_row):
        y = to_gregorian_year(val)
        if y is not None:
            if first_year_col_idx is None:
                first_year_col_idx = j
            year_cols.append(j)
            year_names.append(str(y))

    if not year_cols:
        log(debug, "  ⚠ no year columns detected; returning empty tidy df")
        return pd.DataFrame(columns=["item_th", "orig_index"])

    # หา item column = คอลัมน์แรกก่อนคอลัมน์ปีตัวแรก ที่มีข้อมูล (อย่างน้อย 2 แถว)
    item_col_idx = None
    if first_year_col_idx is not None:
        for j in range(first_year_col_idx - 1, -1, -1):
            col_vals = df_raw.iloc[hdr+1:, j]
            non_na = col_vals[~col_vals.isna()]
            if len(non_na) >= 2:
                item_col_idx = j
                break
    if item_col_idx is None:
        item_col_idx = 0  # fallback
        log(debug, "  ⚠ item column fallback to index 0")

    use_cols = [item_col_idx] + year_cols
    body = df_raw.iloc[hdr+1:, use_cols].copy()
    body.columns = ["item_th"] + year_names

    def clean_item(v: Any) -> str:
        if is_none_or_nan(v):
            return "unknown"
        s = normalize_spaces(str(v))
        return s if s else "unknown"

    body["item_th"] = body["item_th"].map(clean_item)

    # แปลงตัวเลข: '-'/0 => 0.0, อื่น ๆ ตามจริง; ถ้าแปลงไม่ได้ → None
    for c in year_names:
        body[c] = body[c].map(to_float_or_zero)

    # กรองแถวที่ทุกปีเป็น None (แต่ 0.0 จะไม่ถูกตัด)
    has_any = body[year_names].apply(lambda r: any(v is not None for v in r), axis=1)
    body = body[has_any].copy().reset_index(drop=True)

    # คงลำดับเดิม
    body["orig_index"] = body.index

    log(debug, f"  ✔ tidy df shape={body.shape}; cols={list(body.columns)}")
    return body


# ---------------- Build JSON ---------------- #

def dataframe_to_year_json(df: pd.DataFrame, tax_id: str, debug: bool) -> Dict[str, List[Dict[str, Any]]]:
    """
    แปลง wide table -> dict[year] = [ {item, item_en, amount, ...}, ... ]
    เรียงตาม orig_index (ลำดับในไฟล์)
    - ค่าที่เป็น 0.0 จะถูกใส่ลง JSON (เพราะไม่ใช่ None)
    """
    if df.empty:
        return {}

    years = [c for c in df.columns if c not in ("item_th", "orig_index")]
    out: Dict[str, List[Dict[str, Any]]] = {y: [] for y in years}

    for _, row in df.iterrows():
        item_th = str(row["item_th"]).strip()
        item_en = map_item_th_to_en(item_th)
        order = int(row["orig_index"])

        for y in years:
            val = row[y]
            if is_none_or_nan(val):
                continue
            out[y].append({
                "item": item_th,
                "item_en": item_en,
                "amount": float(val),   # '-' / 0 ถูกแปลงเป็น 0.0 แล้ว
                "pct_change": None,     # งบกำไรขาดทุนทั่วไปไม่มี %change
                "tax_id": tax_id,
                "_order": order,
            })

    # เรียงตามลำดับเดิม
    for y in years:
        out[y].sort(key=lambda d: d["_order"])
        for d in out[y]:
            d.pop("_order", None)

    return out


# ---------------- Orchestration ---------------- #

TAX_ID_RE = re.compile(r"(?P<tax>\d{13})_income\.(?:xlsx|xls)$", re.IGNORECASE)

def extract_tax_id_from_name(name: str) -> Optional[str]:
    m = TAX_ID_RE.search(name)
    return m.group("tax") if m else None

def process_one_file(path: Path, outdir: Path, debug: bool):
    tax_id = extract_tax_id_from_name(path.name)
    if not tax_id:
        log(debug, f"  ⚠ skip (cannot parse tax id): {path.name}")
        return

    print(f"\n▶ Processing: {path.name} (tax_id={tax_id})")
    print(f"  ↪ detected suffix: {path.suffix.lower().lstrip('.')}")

    df_raw = read_income_table(path, debug)
    df_tidy = tidy_income_table(df_raw, debug)
    years_json = dataframe_to_year_json(df_tidy, tax_id, debug)

    out_path = outdir / f"{tax_id}_income.json"
    outdir.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(years_json, f, ensure_ascii=False, indent=2)

    total_rows = sum(len(v) for v in years_json.values())
    print(f"  ✔ wrote: {out_path} (years={list(years_json.keys())}, total_rows={total_rows})")

def process_folder(in_dir: Path, outdir: Path, debug: bool):
    files = sorted([p for p in in_dir.glob("*_income.*") if p.suffix.lower() in {".xls", ".xlsx"}])
    if not files:
        print("No *_income.xls/xlsx files found.")
        return
    for p in files:
        process_one_file(p, outdir, debug)


# ---------------- CLI ---------------- #

def main():
    ap = argparse.ArgumentParser(description="Read DBD *_income Excel → JSON (by year, keep original order)")
    ap.add_argument("--folder", required=True, help="input folder containing *_income.xls/xlsx")
    ap.add_argument("--outdir", required=True, help="output folder for <tax_id>_income.json")
    ap.add_argument("--debug", action="store_true", help="verbose logs")
    args = ap.parse_args()

    in_dir = Path(args.folder).expanduser().resolve()
    out_dir = Path(args.outdir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    process_folder(in_dir, out_dir, debug=args.debug)


if __name__ == "__main__":
    main()
