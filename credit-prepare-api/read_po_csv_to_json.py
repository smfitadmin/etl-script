#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
read_po_csv_to_json.py

- อ่าน CSV จาก raw_data/po/<file>.csv
- Header หลัก: แถว 5 (index 4), ข้อมูล: แถว 6 (index 5)
- ดึง Buyer จาก B3 (0-based: [2,1])
- ดึงวันที่หัวรายงาน:
    * PO Report Date  จาก D4 (0-based: [3,3])  -> m/d/yyyy
    * PO Received Date จาก F4 (0-based: [3,5])  -> m/d/yyyy
  ถ้าไม่มีหรือ parse ไม่ได้ -> null
- ตัดเฉพาะ "แถวท้าย" ที่เป็น Total/Grand Total/Subtotal/รวม/ยอดรวม/รวมทั้งสิ้น หรือแถวว่างจริง ๆ
- โครงสร้าง JSON ต่อแถว:
  {
    "PO No.", "Buyer Code", "Buyer Name", "Supplier Code", "Supplier Name",
    "Order Date", "Send Date", "Delivery Date", "PO Report Date", "PO Received Date",
    "Amount (PO Include VAT)", "Status"
  }
- ฟอร์แมตวัน:
    * Order/Delivery/PO Report Date/PO Received Date -> YYYY-MM-DD
    * Send Date -> YYYY-MM-DD HH:mm:ss (24 ชม.)
- บันทึกผลไว้ที่ processed_data/po/<same_name>.json
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# ---------- IO helpers ----------
def read_csv_any_encoding(path: Path) -> pd.DataFrame:
    for enc in ["utf-8-sig", "utf-8", "cp874", "tis-620", "latin1"]:
        try:
            return pd.read_csv(path, header=None, dtype=str, encoding=enc)
        except Exception:
            continue
    raise RuntimeError(f"Cannot read CSV with known encodings: {path}")

def _strip_all(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\n", " ")).strip()


# ---------- Buyer from B3 ----------
def extract_buyer_from_b3(raw_df: pd.DataFrame) -> Dict[str, Any]:
    buyer_text: Optional[str] = None
    try:
        buyer_text = str(raw_df.iat[2, 1]).strip()  # B3 -> [2,1]
    except Exception:
        pass

    out: Dict[str, Any] = {}
    if not buyer_text or buyer_text.lower() == "nan":
        return out

    out["buyer_b3"] = buyer_text

    # Buyer Code (เลขยาว 10–20 หลัก)
    m_code = re.search(r"(\d{10,20})", buyer_text.replace(" ", ""))
    if m_code:
        out["Buyer Code"] = m_code.group(1)

    # Buyer Name จากข้อความที่เหลือ
    name = buyer_text
    if m_code:
        name = re.sub(r"\(?%s\)?" % re.escape(m_code.group(1)), "", name)
    if ":" in name:
        name = name.split(":", 1)[1]
    name = re.sub(r"\s*\(\s*\)\s*", "", name).strip()
    if name:
        out["Buyer Name"] = name

    return out


# ---------- Date parsers ----------
DATE_TOKEN_MDYYYY = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})")  # m/d/yyyy or m-d-yyyy

def parse_date_ddmmyyyy_to_iso(d: Optional[str]) -> Optional[str]:
    if not d or pd.isna(d) or str(d).strip() == "":
        return None
    ts = pd.to_datetime(d, dayfirst=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")

def parse_date_mmddyyyy_to_iso(d: Optional[str]) -> Optional[str]:
    """month-first -> YYYY-MM-DD"""
    if not d or pd.isna(d) or str(d).strip() == "":
        return None
    ts = pd.to_datetime(d, dayfirst=False, errors="coerce")
    if pd.isna(ts):
        # ลองจับ substring m/d/yyyy
        s = str(d).strip()
        m = DATE_TOKEN_MDYYYY.search(s)
        if m:
            mo, day, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if y < 100:  # เผื่อ yy -> yyyy
                y += 2000 if y < 50 else 1900
            return f"{y:04d}-{mo:02d}-{day:02d}"
        return None
    return ts.strftime("%Y-%m-%d")

def parse_send_datetime_to_iso(dt: Optional[str]) -> Optional[str]:
    if not dt or pd.isna(dt) or str(dt).strip() == "":
        return None
    s = str(dt).replace("\n", " ").strip()
    # รองรับ "m/d/yyyy HH:MM:SS AM/PM" และเคส "14:44:07 PM"
    m = re.search(
        r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})(?:\s*([AaPp][Mm]))?",
        s
    )
    if not m:
        ts = pd.to_datetime(s, errors="coerce")  # ปล่อยให้ pandas เดา
        if pd.isna(ts):
            # try month-first explicitly
            ts = pd.to_datetime(s, dayfirst=False, errors="coerce")
        if pd.isna(ts):
            # try day-first as last resort
            ts = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    hh, mm, ss = int(m.group(4)), int(m.group(5)), int(m.group(6))
    ampm = m.group(7).upper() if m.group(7) else None

    if ampm:
        if 1 <= hh <= 11 and ampm == "PM":
            hh += 12
        elif hh == 12 and ampm == "AM":
            hh = 0
        # ถ้า hh >= 13 ปล่อยไว้ (ถือว่าเป็น 24 ชม. อยู่แล้ว)

    return f"{y:04d}-{mo:02d}-{d:02d} {hh:02d}:{mm:02d}:{ss:02d}"


# ---------- Header dates from D4/F4 (month-first) ----------
def extract_mmddyyyy_from_cell(raw_df: pd.DataFrame, row_idx: int, col_idx: int) -> Optional[str]:
    """
    อ่านค่าจากตำแหน่ง cell (0-based) แล้ว parse เป็น YYYY-MM-DD โดย month-first
    ถ้าไม่มี/parse ไม่ได้ -> None
    """
    try:
        text = str(raw_df.iat[row_idx, col_idx]).strip()
    except Exception:
        return None
    if not text or text.lower() == "nan":
        return None
    # ดึง substring รูปแบบ m/d/yyyy ถ้ามีตัวหนังสือปน
    m = DATE_TOKEN_MDYYYY.search(text)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000 if y < 50 else 1900
        return f"{y:04d}-{mo:02d}-{d:02d}"
    # เผื่อเคสที่เป็น date เต็ม ๆ แต่ไม่เจอด้วย regex
    return parse_date_mmddyyyy_to_iso(text)


# ---------- DataFrame with proper columns ----------
def build_data_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    header_row_idx = 4  # row 5
    data_start_idx = 5  # row 6

    headers_raw = list(raw_df.iloc[header_row_idx].fillna(""))
    headers = [_norm_text(str(x)) for x in headers_raw]

    # make headers unique
    seen: Dict[str, int] = {}
    uniq_headers: List[str] = []
    for h in headers:
        key = h if h else "col"
        if key not in seen:
            seen[key] = 1
            uniq_headers.append(key)
        else:
            seen[key] += 1
            uniq_headers.append(f"{key}_{seen[key]}")

    df = raw_df.iloc[data_start_idx:].reset_index(drop=True)
    df.columns = uniq_headers
    df = df.dropna(how="all")
    df = _strip_all(df)

    # บางรายงานมีหัวคอลัมน์ซ้ำในแถวข้อมูลแรก → ตรวจแล้วรีฉลาก
    expected_labels = {
        "PO No.", "Supplier Code", "Supplier Name", "Order Date",
        "Send Date", "Delivery Date", "Amount (PO Include VAT)", "Status"
    }
    if not df.empty:
        first = {c: _norm_text(str(df.iloc[0][c])) for c in df.columns}
        if len(expected_labels.intersection(set(first.values()))) >= 3:
            new_cols = list(df.columns)
            rev = {v: k for k, v in first.items()}
            for lab in expected_labels:
                if lab in rev:
                    idx = new_cols.index(rev[lab])
                    new_cols[idx] = lab
            df.columns = new_cols
            df = df.iloc[1:].reset_index(drop=True)

    return df


# ---------- Tail trimming (Total/Grand Total/Empty) ----------
TOTAL_PAT = re.compile(r"\b(total|grand\s*total|sub\s*total)\b", re.IGNORECASE)
THAI_TOTAL_SUBSTRS = ("รวมทั้งสิ้น", "ยอดรวม", "รวม")

def _is_empty_value(v: Any) -> bool:
    return (v is None) or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v.strip() == "")

def _row_is_empty(row: pd.Series) -> bool:
    return all(_is_empty_value(v) for v in row.values)

def _row_has_total_keyword(row: pd.Series) -> bool:
    for v in row.values:
        if isinstance(v, str):
            t = v.strip()
            if TOTAL_PAT.search(t):
                return True
            low = t.lower()
            if any(substr in low for substr in THAI_TOTAL_SUBSTRS):
                return True
    return False

def drop_trailing_totals_or_empty(df: pd.DataFrame) -> pd.DataFrame:
    """ลบเฉพาะแถวท้ายที่เป็นว่างหรือมีคำว่า total/grand total/subtotal/รวม/ยอดรวม/รวมทั้งสิ้น ต่อเนื่องจากท้ายตาราง"""
    if df.empty:
        return df
    end = len(df)
    while end > 0:
        row = df.iloc[end - 1]
        if _row_is_empty(row) or _row_has_total_keyword(row):
            end -= 1
        else:
            break
    if end < len(df):
        df = df.iloc[:end].reset_index(drop=True)
    return df


# ---------- Row → JSON ----------
def row_to_output(row: pd.Series, buyer: Dict[str, Any], header_dates: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "PO No.": row.get("PO No."),
        "Buyer Code": buyer.get("Buyer Code"),
        "Buyer Name": buyer.get("Buyer Name"),
        "Supplier Code": row.get("Supplier Code"),
        "Supplier Name": row.get("Supplier Name"),
        "Order Date": parse_date_ddmmyyyy_to_iso(row.get("Order Date")),          # dd/mm/yyyy -> iso
        "Send Date": parse_send_datetime_to_iso(row.get("Send Date")),            # keep key even if None
        "Delivery Date": parse_date_ddmmyyyy_to_iso(row.get("Delivery Date")),    # dd/mm/yyyy -> iso
        "PO Received From Date": header_dates.get("PO Received From Date"),                     # from D4 (mm/dd/yyyy)
        "PO Received To Date": header_dates.get("PO Received To Date"),                 # from F4 (mm/dd/yyyy)
        "Amount (PO Include VAT)": _parse_amount(row.get("Amount (PO Include VAT)")),
        "Status": row.get("Status"),
    }

def _parse_amount(s: Optional[str]) -> Optional[float]:
    if s is None or pd.isna(s):
        return None
    txt = str(s).strip()
    if txt == "":
        return None
    txt = txt.replace(",", "").replace(" ", "")
    try:
        return float(txt)
    except Exception:
        return None


# ---------- Convert one file ----------
def convert_one(csv_path: Path) -> Path:
    raw = read_csv_any_encoding(csv_path)
    buyer = extract_buyer_from_b3(raw)

    # Header dates (month-first)
    po_report_date = extract_mmddyyyy_from_cell(raw, 3, 3)   # D4
    po_received_date = extract_mmddyyyy_from_cell(raw, 3, 5) # F4
    header_dates = {
        "PO Received From Date": po_report_date,         # None if missing/unparseable
        "PO Received To Date": po_received_date,     # None if missing/unparseable
    }

    df = build_data_df(raw)
    df = drop_trailing_totals_or_empty(df)

    records: List[Dict[str, Any]] = [row_to_output(r, buyer, header_dates) for _, r in df.iterrows()]

    out_dir = Path("processed_data/po")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / csv_path.with_suffix(".json").name

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(records)} records -> {out_path}")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Convert PO CSV to JSON (buyer B3, D4/F4 dates m/d/yyyy, strict formats, tail-trim totals/empty).")
    ap.add_argument("--csv", required=True, help='e.g. raw_data/po/po_detail_report_20251007_2050363.csv')
    args = ap.parse_args()

    src = Path(args.csv)
    if not src.exists():
        raise FileNotFoundError(f"CSV not found: {src}")

    convert_one(src)


if __name__ == "__main__":
    main()
