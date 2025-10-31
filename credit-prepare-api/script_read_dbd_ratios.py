#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
script_read_dbd_ratios.py (v2.2)

- item (ไทย): เหมือนในไฟล์ 100% (trim อย่างเดียว)
- item_en (อังกฤษ): snake_case ตาม mapping ที่กำหนด เช่น
  "return_on_assets_roa_percent", "debt_to_working_capital_ratio_times"
- '-', '–', '—', 0, '0', '0.0' -> 0.0
- '12.3%' -> 12.3 (คงเป็นร้อยละ ไม่หาร 100)
- เลือกคอลัมน์รายการแบบให้คะแนน เพื่อเลี่ยงเลือกผิดคอลัมน์
- กัน "None"/NaN ทั้งใน item และ amount ไม่ให้หลุดลง JSON

Usage:
  pip install "xlrd==1.2.0" pandas openpyxl pandas-calamine
  python script_read_dbd_ratios.py --folder ./downloads --outdir ./out_json --debug
"""

from __future__ import annotations
import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# ========= Utils ========= #

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

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

# ตัวเลขหรือเปอร์เซนต์ เช่น "1,234.56", " 12.3 % ", "-5.0%", "8,765"
_NUM_OR_PCT_RE = re.compile(r"[-+]?\d+(?:[,\s]\d{3})*(?:\.\d+)?\s*%?")

def _is_dash_or_zero_str(s: str) -> bool:
    s2 = normalize_spaces(s).replace(",", "")
    return s2 in {"-", "–", "—", "0", "+0", "-0", "0.0", "+0.0", "-0.0"}

def to_float_or_zero_keep_percent_value(x: Any) -> Optional[float]:
    if is_none_or_nan(x):
        return None
    if isinstance(x, (int, float)):
        return 0.0 if float(x) == 0.0 else float(x)
    s = str(x)
    if _is_dash_or_zero_str(s):
        return 0.0
    s_nospace = s.replace(" ", "")
    m = _NUM_OR_PCT_RE.search(s_nospace)
    if not m:
        return None
    token = m.group(0).replace("%", "").replace(",", "")
    try:
        val = float(token)
        return 0.0 if val == 0.0 else val
    except Exception:
        return None

def to_gregorian_year(val: Any) -> Optional[int]:
    if is_none_or_nan(val):
        return None
    try:
        y = int(float(str(val).strip()))
    except Exception:
        return None
    if y >= 2400:
        y -= 543
    return y if 1900 <= y <= 2100 else None

# ========= Mapping ========= #

TH_TO_EN_FULL: Dict[str, str] = {
    "อัตราผลตอบแทนจากสินทรัพย์รวม(ROA) (%)": "return_on_assets_percent",
    "อัตราผลตอบแทนจากส่วนของผู้ถือหุ้น(ROE) (%)": "return_on_equity_percent",
    "ผลตอบแทนจากกำไรขั้นต้นต่อรายได้รวม (%)": "gross_profit_margin_percent",
    "ผลตอบแทนจากกำไรการดำเนินงานต่อรายได้รวม (%)": "operating_profit_margin_percent",
    "ผลตอบแทนจากกำไรสุทธิต่อรายได้รวม (%)": "net_profit_margin_percent",
    "อัตราส่วนทุนหมุนเวียน(เท่า)": "current_ratio_times",
    "อัตราการหมุนเวียนของลูกหนี้ (เท่า)": "accounts_receivable_turnover_times",
    "อัตราการหมุนเวียนของสินค้าคงเหลือ (เท่า)": "inventory_turnover_times",
    "อัตราการหมุนเวียนของเจ้าหนี้ (เท่า)": "accounts_payable_turnover_times",
    "อัตราการหมุนเวียนของสินทรัพย์รวม (เท่า)": "total_asset_turnover_times",
    "อัตราค่าใช้จ่ายการดำเนินงานต่อรายได้รวม (%)": "operating_expense_ratio_percent",
    "อัตราส่วนสินทรัพย์รวมต่อส่วนของผู้ถือหุ้น (เท่า)": "total_assets_to_shareholders_equity_ratio_times",
    "อัตราส่วนหนี้สินรวมต่อสินทรัพย์รวม (เท่า)": "total_liabilities_to_total_assets_ratio_times",
    "อัตราส่วนหนี้สินรวมต่อส่วนของผู้ถือหุ้น (เท่า)": "debt_to_equity_ratio_times",
    "อัตราส่วนหนี้สินรวมต่อทุนดำเนินงาน (เท่า)": "debt_to_working_capital_ratio_times",
}

def map_item_th_to_en(th_name: Any) -> str:
    if is_none_or_nan(th_name):
        return "unknown"
    s = str(th_name).strip()
    if s in TH_TO_EN_FULL:
        return TH_TO_EN_FULL[s]
    s_norm = normalize_spaces(s)
    for k, v in TH_TO_EN_FULL.items():
        if normalize_spaces(k) == s_norm:
            return v
    up = s.upper()
    if "ROA" in up: return "return_on_assets_roa_percent"
    if "ROE" in up: return "return_on_equity_roe_percent"
    if "กำไรขั้นต้น" in s: return "gross_profit_margin_percent"
    if "กำไรการดำเนินงาน" in s or "กำไรจากการดำเนินงาน" in s: return "operating_profit_margin_percent"
    if "กำไรสุทธิ" in s: return "net_profit_margin_percent"
    if "ทุนหมุนเวียน" in s: return "current_ratio_times"
    if "ลูกหนี้" in s: return "accounts_receivable_turnover_times"
    if "สินค้าคงเหลือ" in s: return "inventory_turnover_times"
    if "เจ้าหนี้" in s: return "accounts_payable_turnover_times"
    if "สินทรัพย์รวม" in s and "หมุนเวียน" in s: return "total_asset_turnover_times"
    if "ค่าใช้จ่ายการดำเนินงานต่อรายได้รวม" in s: return "operating_expense_ratio_percent"
    if "สินทรัพย์รวมต่อส่วนของผู้ถือหุ้น" in s: return "total_assets_to_shareholders_equity_ratio_times"
    if "หนี้สินรวมต่อสินทรัพย์รวม" in s: return "total_liabilities_to_total_assets_ratio_times"
    if "หนี้สินรวมต่อส่วนของผู้ถือหุ้น" in s: return "debt_to_equity_ratio_times"
    if "หนี้สินรวมต่อทุนดำเนินงาน" in s: return "debt_to_working_capital_ratio_times"
    return "unknown"

# ========= Readers ========= #

def detect_ooxml_zip_signature(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except Exception:
        return False

def read_ratios_table(path: Path, sheet: Optional[str], debug: bool) -> pd.DataFrame:
    suffix = path.suffix.lower()
    is_zip = detect_ooxml_zip_signature(path)
    log(debug, f"  ↪ detected suffix: {suffix}, is_zip={is_zip}")
    sheet_name = sheet if sheet is not None else 0

    engines = (["openpyxl"] if is_zip else []) + ["xlrd", "calamine", None]
    tried = []
    for engine in engines:
        try:
            df = pd.read_excel(path, engine=engine, header=None, sheet_name=sheet_name)
            log(debug, f"  ✔ read_excel({engine or 'auto'}) ok: shape={df.shape}")
            return df
        except Exception as e:
            tried.append((engine or "auto", str(e)))
            log(debug, f"  ⚠ failed read_excel({engine or 'auto'}): {e}")
    raise RuntimeError(f"Cannot read Excel file: {path.name}; tried={tried}")

# ========= Tidy ========= #

def detect_header_row(df: pd.DataFrame, debug: bool) -> int:
    best_idx, best_count = 0, -1
    for i in range(min(len(df), 40)):
        cnt = sum(1 for cell in df.iloc[i] if to_gregorian_year(cell) is not None)
        if cnt > best_count:
            best_idx, best_count = i, cnt
    log(debug, f"  ↪ header row index={best_idx} (year-like cells={best_count})")
    return best_idx

def _looks_like_label(x: Any) -> bool:
    if is_none_or_nan(x):
        return False
    s = normalize_spaces(str(x))
    if s == "":
        return False
    if _NUM_OR_PCT_RE.fullmatch(s.replace(" ", "")):
        return False
    return True

def tidy_ratios_table(df_raw: pd.DataFrame, debug: bool) -> pd.DataFrame:
    hdr_idx = detect_header_row(df_raw, debug)
    header_row = df_raw.iloc[hdr_idx].tolist()

    # หา year cols
    year_cols: List[int] = []
    year_names: List[str] = []
    for j, cell in enumerate(header_row):
        y = to_gregorian_year(cell)
        if y is not None:
            year_cols.append(j)
            year_names.append(str(y))

    if not year_cols:
        log(debug, "⚠ no year cols; return empty")
        return pd.DataFrame(columns=["item_th"])

    body_all = df_raw.iloc[hdr_idx + 1:].copy()

    # ===== เลือกคอลัมน์รายการแบบให้คะแนน =====
    n_cols = df_raw.shape[1]
    non_year_candidates = [c for c in range(n_cols) if c not in year_cols]
    best_col = None
    best_score = -1
    sample = body_all.head(80)

    # ใช้ mapping ไทยเพื่อช่วยบอกคะแนน
    th_keys = set(TH_TO_EN_FULL.keys())

    for c in non_year_candidates:
        col_series = sample.iloc[:, c]
        label_like = col_series.map(_looks_like_label).sum()

        def in_map(x):
            if is_none_or_nan(x):
                return False
            s = normalize_spaces(str(x))
            return s in th_keys or normalize_spaces(s) in {normalize_spaces(k) for k in th_keys}

        mapped_cnt = col_series.map(in_map).sum()
        # ถ้า cell เป็น label-like มาก + เจอใน mapping จะได้คะแนนสูง
        score = int(label_like) * 2 + int(mapped_cnt) * 3
        if score > best_score:
            best_score = score
            best_col = c

    if best_col is None:
        # fallback: คอลัมน์แรกที่ไม่ใช่ปี
        best_col = non_year_candidates[0]

    item_col_idx = best_col
    log(debug, f"  ↪ chosen item_col_idx={item_col_idx} (score={best_score})")

    # ใช้เฉพาะคอลัมน์ item + ปี
    use_cols = [item_col_idx] + year_cols
    body = body_all.iloc[:, use_cols].copy()
    body.columns = ["item_th"] + year_names

    # เก็บชื่อไทยตามไฟล์ → trim + ffill (รองรับ merged cells)
    body["item_th"] = body["item_th"].map(lambda x: str(x).strip() if not is_none_or_nan(x) else None)
    body["item_th"] = body["item_th"].ffill()

    # กรอง noise เช่น "หน่วย", "หมายเหตุ" และแถวที่ยังว่าง
    body = body[~body["item_th"].str.contains(r"^(หน่วย|หมายเหตุ)", na=False)].copy()
    body = body[body["item_th"].notna()].copy()
    body = body[body["item_th"].map(lambda s: str(s).strip() != "")].copy()

    # แปลงค่าตัวเลข
    for y in year_names:
        body[y] = body[y].map(to_float_or_zero_keep_percent_value)

    # เก็บเฉพาะแถวที่มีข้อมูลอย่างน้อยหนึ่งปี (รวม 0.0 ด้วย)
    has_any = body[year_names].apply(lambda r: any(not is_none_or_nan(v) for v in r), axis=1)
    body = body[has_any].copy().reset_index(drop=True)

    # ลำดับเดิม
    body["orig_index"] = body.index
    log(debug, f"✔ tidy shape={body.shape}; cols={list(body.columns)}")
    return body

# ========= Convert to JSON ========= #

def dataframe_to_year_json(df: pd.DataFrame, tax_id: str, debug: bool) -> Dict[str, List[Dict[str, Any]]]:
    years = [c for c in df.columns if c not in ("item_th", "orig_index")]
    out: Dict[str, List[Dict[str, Any]]] = {y: [] for y in years}

    for _, row in df.iterrows():
        if is_none_or_nan(row["item_th"]):
            # กัน item ว่าง (จะไม่ปล่อย "None" ลง JSON)
            continue
        item_th = str(row["item_th"]).strip()
        if item_th == "":
            continue

        item_en = map_item_th_to_en(item_th)
        order = int(row["orig_index"])

        for y in years:
            val = row[y]
            if is_none_or_nan(val):
                continue
            out[y].append({
                "item": item_th,          # ไทยตามไฟล์ (trim)
                "item_en": item_en,       # อังกฤษ snake_case
                "amount": float(val),     # ไม่ปล่อย NaN
                "pct_change": None,
                "tax_id": tax_id,
                "_order": order
            })

    # เรียงตามลำดับเดิมในไฟล์
    for y in years:
        out[y].sort(key=lambda d: d["_order"])
        for d in out[y]:
            d.pop("_order", None)
    return out

# ========= Orchestration ========= #

def process_one_file(path: Path, outdir: Path, sheet: Optional[str], debug: bool):
    m = re.search(r"(?P<tax>\d{13})", path.name)
    if not m:
        log(debug, f"skip (no tax id in name): {path.name}")
        return
    tax_id = m.group("tax")

    print(f"\n▶ Processing {path.name} (tax_id={tax_id})")
    df_raw = read_ratios_table(path, sheet, debug)
    df_tidy = tidy_ratios_table(df_raw, debug)
    data = dataframe_to_year_json(df_tidy, tax_id, debug)

    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"{tax_id}_ratios.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    total_rows = sum(len(v) for v in data.values())
    print(f"✔ wrote {out_path} (years={list(data.keys())}, total_rows={total_rows})")

def main():
    ap = argparse.ArgumentParser(description="Read DBD *_ratios Excel → JSON (by year)")
    ap.add_argument("--folder", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    folder = Path(args.folder).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in folder.glob("*_ratios.*") if p.suffix.lower() in {".xls", ".xlsx"}])
    if not files:
        print("No *_ratios.xls/xlsx files found.")
        return

    for f in files:
        process_one_file(f, outdir, args.sheet, args.debug)

if __name__ == "__main__":
    main()
