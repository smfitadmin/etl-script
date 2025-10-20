#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sale Supplier OCR → JSON-only (OpenTyphoon-based)

Usage:
    python ocr_sale_supplier_pipeline.py --api-key YOUR_API_KEY
    # default input:  raw_data/sale_supplier
    # default output: processed_data/sale_supplier

Options:
    --folder <path>               # optional; default: raw_data/sale_supplier
    --pages '[1,2,3]'             # optional page list
    --task-type structure         # default
    --max-tokens 16000            # default
    --temperature 0.1             # default
    --top-p 0.6                   # default
    --repetition-penalty 1.2      # default
"""

import os
import re
import json
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==============================
# OCR Extraction (OpenTyphoon)
# ==============================
def extract_text_from_image(image_path, api_key, task_type, max_tokens, temperature, top_p, repetition_penalty, pages=None):
    url = "https://api.opentyphoon.ai/v1/ocr"
    with open(image_path, 'rb') as file:
        files = {'file': file}
        data = {
            'task_type': task_type,
            'max_tokens': str(max_tokens),
            'temperature': str(temperature),
            'top_p': str(top_p),
            'repetition_penalty': str(repetition_penalty)
        }
        if pages:
            data['pages'] = json.dumps(pages)

        headers = {'Authorization': f'Bearer {api_key}'}
        resp = requests.post(url, files=files, data=data, headers=headers)
        if resp.status_code != 200:
            print(f"[ERROR] OCR API {resp.status_code} for {os.path.basename(image_path)}")
            print(resp.text)
            return None

        result = resp.json()
        extracted_texts = []
        for page_result in result.get('results', []):
            if page_result.get('success') and page_result.get('message'):
                content = page_result['message']['choices'][0]['message']['content']
                try:
                    parsed = json.loads(content)
                    text = parsed.get('html', parsed.get('natural_text', content))
                except json.JSONDecodeError:
                    text = content
                extracted_texts.append(text)
            else:
                print(f"[WARN] Page error in {os.path.basename(image_path)}: {page_result.get('error')}")
        return "\n".join(extracted_texts)


# ==============================
# Helpers
# ==============================
THAI_MONTHS = {
    "มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03", "เมษายน": "04",
    "พฤษภาคม": "05", "มิถุนายน": "06", "กรกฎาคม": "07", "สิงหาคม": "08",
    "กันยายน": "09", "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12",
    # common OCR shortenings
    "กุมภา": "02", "มีค": "03", "เมย": "04", "มิย": "06", "กค": "07",
    "สค": "08", "กย": "09", "ตค": "10", "พย": "11", "ธค": "12"
}

def convert_date_round_dd_mm_yyyy(date_str: str) -> str:
    """Convert DD.MM.YYYY (B.E. or C.E.) → YYYY-MM-DD; else return original."""
    try:
        s = str(date_str).strip()
        if not s or s.lower() == "nan":
            return ""
        parts = s.split(".")
        if len(parts) == 3:
            d, m, y = parts
            d, m, y = int(d), int(m), int(y)
            if y > 2400:  # Buddhist Era → C.E.
                y -= 543
            return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        pass
    return date_str

def normalize_columns(headers, rows):
    """Pad/trim headers and each row to the same length."""
    max_cols = max(len(headers), max((len(r) for r in rows), default=0))
    def norm(arr):
        if len(arr) < max_cols:
            return arr + [""] * (max_cols - len(arr))
        if len(arr) > max_cols:
            return arr[:max_cols]
        return arr
    headers = norm(headers) if headers else [f"col_{i+1}" for i in range(max_cols)]
    rows = [norm(r) for r in rows]
    return headers, rows

def is_total_line(cells):
    txt = " ".join([str(c) for c in cells]).lower()
    return re.search(r"(?:\btotal\b|grand\s*total|รวมยอดทั้งหมด)", txt) is not None

def clean_cell(cell: str):
    s = str(cell)
    # If cell contains "total ..." keep first number
    if re.search(r'(?i)total', s):
        nums = re.findall(r'\d+(?:\.\d+)?', s.replace(",", ""))
        return nums[0] if nums else ''
    # if OCR glued two numbers, keep first
    if re.findall(r'\d+(?:\.\d+)?\s+\d+(?:\.\d+)?', s):
        first = re.findall(r'\d+(?:\.\d+)?', s.replace(",", ""))
        return first[0] if first else s
    return s

def to_float_robust(s: str) -> float:
    """
    Robust numeric parser for OCR strings:
      - '1,357.000'     → 1357.000
      - '1.357.000'     → 1357.000  (last '.' is decimal, previous are thousands)
      - '1.357.000,50'  → 1357000.50 (comma as decimal in some formats)
      - '1 357,000'     → 1357.000
    Rules:
      - If both '.' and ',' present → assume ',' thousands, '.' decimal (TH/US)
      - If only '.' and more than one → last '.' is decimal, previous are thousands
      - If only ',' and more than one → last ',' is decimal, previous are thousands
      - If only one comma and no dot → comma is decimal
      - Else: remove ',' (thousands), keep '.' as decimal
    """
    if s is None:
        return 0.0
    t = re.sub(r"[^\d\.,\s]", "", str(s)).strip()
    if not t:
        return 0.0
    t = t.replace(" ", "")
    dots = t.count(".")
    commas = t.count(",")

    if dots >= 1 and commas >= 1:
        # ',' thousands, '.' decimal
        t = t.replace(",", "")
        try:
            return float(t)
        except ValueError:
            pass

    if commas == 0 and dots >= 2:
        last = t.rfind(".")
        left = t[:last].replace(".", "")
        right = t[last+1:]
        t = left + ("." + right if right != "" else "")
        try:
            return float(t)
        except ValueError:
            return 0.0

    if dots == 0 and commas >= 2:
        last = t.rfind(",")
        left = t[:last].replace(",", "")
        right = t[last+1:]
        t = left + ("." + right if right != "" else "")
        try:
            return float(t)
        except ValueError:
            return 0.0

    if commas == 1 and dots == 0:
        t = t.replace(",", ".")
        try:
            return float(t)
        except ValueError:
            return 0.0

    t = t.replace(",", "")
    try:
        return float(t)
    except ValueError:
        return 0.0


# ==============================
# Non-Table Metadata (supplier-flavored)
# ==============================
def parse_non_table_metadata(ocr_output: str) -> dict:
    """
    Extract topic/vendor/period from free text.
      - "รายงานการขายสินค้า - แยกตามผู้ขาย/ผู้จำหน่าย/Supplier/Invoice"
      - "Vendor 2040334 / <NAME> (2040334)"
      - "รอบวันที่ 1 - 31 ธันวาคม 2567"
    """
    soup = BeautifulSoup(ocr_output, "html.parser")
    for table in soup.find_all("table"):
        table.decompose()
    text_content = soup.get_text(separator="\n")
    lines = [line.strip() for line in text_content.splitlines() if line.strip()]
    if not lines:
        return {}

    text = " ".join(lines)

    # topic
    m_topic = re.search(
        r"(รายงานการขายสินค้า\s*-\s*แยกตาม\s*(?:ผู้ขาย|ผู้จำหน่าย|Supplier|ซัพพลายเออร์|Invoice))",
        text
    )
    topic = m_topic.group(1).strip() if m_topic else ""

    # round period
    m_period = re.search(r"รอบวันที่\s*(\d{1,2})\s*-\s*(\d{1,2})\s*([ก-๙]+)\s*(\d{4})", text)
    if m_period:
        d1, d2, month_th, year = m_period.groups()
        mnum = THAI_MONTHS.get(month_th.strip(), "00")
        start_round_date = f"{int(d1):02d}.{mnum}.{year}"
        end_round_date   = f"{int(d2):02d}.{mnum}.{year}"
    else:
        start_round_date = end_round_date = ""

    # vendor
    m_vendor = re.search(r"#?\s*Vendor\s*(\d+)\s*/\s*([^\(]+)\s*\(?(\d+)?\)?", text, flags=re.IGNORECASE)
    if m_vendor:
        number_company = m_vendor.group(1)
        name_company = m_vendor.group(2).strip()
    else:
        number_company = name_company = ""

    return {
        "topic": topic,
        "start_round_date": start_round_date,
        "end_round_date": end_round_date,
        "supplier_name": name_company,
        "supplier_num": str(number_company) if number_company else ""
    }


# ==============================
# Table Parsing → DataFrame
# ==============================
def parse_tables_to_df(ocr_output: str, pdf_basename: str):
    soup = BeautifulSoup(ocr_output, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        print(f"[WARN] No tables found in {pdf_basename}")
        return None

    dfs = []
    for idx, table in enumerate(tables, start=1):
        # header from first tr (accept th/td)
        first_tr = table.find("tr")
        header_cells = [c.get_text(strip=True) for c in first_tr.find_all(["th", "td"])] if first_tr else []

        # data rows (skip totals/blank)
        raw_rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if not any(cells):
                continue
            if is_total_line(cells):
                continue
            raw_rows.append(cells)

        if not raw_rows:
            continue

        headers, rows = normalize_columns(header_cells, raw_rows)
        df = pd.DataFrame(rows, columns=headers)

        # clean cells element-wise
        try:
            df = df.map(lambda x: clean_cell(x))
        except AttributeError:
            df = df.applymap(lambda x: clean_cell(x))

        # drop rows containing total words (safety)
        mask_total = df.astype(str).apply(
            lambda r: r.str.contains(r"(?:\btotal\b|grand\s*total|รวมยอดทั้งหมด)", regex=True, case=False, na=False),
            axis=1
        )
        df = df[~mask_total]

        # drop row with empty "ลำดับที่" if present
        if "ลำดับที่" in df.columns:
            df = df[df["ลำดับที่"].astype(str).str.strip() != ""]

        if not df.empty:
            dfs.append(df)

    if not dfs:
        print(f"[WARN] No valid table data in {pdf_basename}")
        return None

    final_df = pd.concat(dfs, ignore_index=True)
    # replace underscores with dots (table headers only)
    final_df.columns = [c.replace("_", ".") for c in final_df.columns]
    final_df = final_df.fillna("")
    return final_df


# ==============================
# DataFrame + Metadata → flattened rows
# ==============================
def dataframe_to_enriched_rows(df: pd.DataFrame, metadata: dict):
    start_round = convert_date_round_dd_mm_yyyy(metadata.get("start_round_date", ""))
    end_round   = convert_date_round_dd_mm_yyyy(metadata.get("end_round_date", ""))

    # numeric precision per field
    numeric_fields_2dp = {"ราคาทุน/หน่วย", "จำนวนเงิน", "ภาษี", "จำนวนเงินสุทธิ", "ยอดสุทธิ", "ยอดรวม"}
    numeric_fields_3dp = {"จำนวนที่ขาย"}

    out = []
    for _, row in df.iterrows():
        row_dict = {}
        for col, val in row.items():
            col_clean = str(col).strip()
            sval = str(val).strip()

            # ---- numeric fields (robust parser) ----
            if col_clean in numeric_fields_2dp or col_clean in numeric_fields_3dp:
                num = to_float_robust(sval)
                row_dict[col_clean] = round(num, 3) if col_clean in numeric_fields_3dp else round(num, 2)
                continue

            # ---- date-ish fields ----
            if ("วันที่" in col_clean) or ("date" in col_clean.lower()):
                row_dict[col_clean] = convert_date_round_dd_mm_yyyy(sval)
                continue

            # ---- default as string ----
            row_dict[col_clean] = sval if sval != "" else ""

        # attach supplier meta
        row_dict["topic"] = metadata.get("topic", "")
        row_dict["start_round_date"] = start_round
        row_dict["end_round_date"] = end_round
        row_dict["supplier_name"] = metadata.get("supplier_name", "")
        row_dict["supplier_num"] = str(metadata.get("supplier_num", "")).strip()

        out.append(row_dict)
    return out


# ==============================
# Batch Processing (Folder)
# ==============================
def process_pdfs_in_folder(folder_path, api_key, task_type, max_tokens, temperature, top_p, repetition_penalty, output_dir, pages=None):
    os.makedirs(output_dir, exist_ok=True)

    for filename in sorted(os.listdir(folder_path)):
        if not filename.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)
        pdf_base = os.path.splitext(filename)[0]
        print(f"\n[INFO] Processing: {filename}")

        ocr_html = extract_text_from_image(
            pdf_path, api_key, task_type, max_tokens, temperature, top_p, repetition_penalty, pages
        )
        if not ocr_html:
            print(f"[ERROR] OCR failed: {filename}")
            continue

        df = parse_tables_to_df(ocr_html, filename)
        meta = parse_non_table_metadata(ocr_html)

        if df is None:
            print(f"[WARN] Skip saving (no table rows): {filename}")
            continue

        rows = dataframe_to_enriched_rows(df, meta)
        out_path = os.path.join(output_dir, f"{pdf_base}.json")
        with open(out_path, "w", encoding="utf-8-sig") as f:
            json.dump(rows, f, ensure_ascii=False, indent=4)

        print(f"[OK] Saved JSON -> {out_path}")


# ==============================
# CLI
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Process Supplier PDFs with OpenTyphoon OCR and export JSON only.")
    parser.add_argument("--folder", default="raw_data/sale_supplier", help="Path to folder containing PDFs.")
    parser.add_argument("--api-key", required=True, help="Your OpenTyphoon API key.")
    parser.add_argument("--task-type", default="structure", help="OCR task type (default: structure).")
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--top-p", type=float, default=0.6)
    parser.add_argument("--repetition-penalty", type=float, default=1.2)
    parser.add_argument("--pages", type=str, help="Optional JSON list of pages to process.")

    # ✅ Default output dir for supplier
    default_output_dir = "processed_data/sale_supplier"

    args = parser.parse_args()
    pages = json.loads(args.pages) if args.pages else None

    process_pdfs_in_folder(
        folder_path=args.folder,
        api_key=args.api_key,
        task_type=args.task_type,
        max_tokens=args.max_tokens,
        temperature=args.temperature,
        top_p=args.top_p,
        repetition_penalty=args.repetition_penalty,
        output_dir=default_output_dir,
        pages=pages
    )


if __name__ == "__main__":
    main()
