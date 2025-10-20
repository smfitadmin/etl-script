#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OCR PDF Processing Module — JSON-only Pipeline (Default output folder)

Usage:
    python pdf_ocr_sale_invoice_to_json.py --folder raw_data/sale_invoice --api-key YOUR_API_KEY

What this script does:
  1) Call OpenTyphoon OCR API on each PDF
  2) Parse tables and non-table metadata from OCR HTML
  3) Clean/normalize rows
  4) Save ONE JSON per PDF to processed_data/sale_invoice/<basename>.json

Notes:
  - No CSV files are created.
  - Robust against column-count mismatch from OCR (pads/truncates rows).
  - Removes total/summary rows.
"""

import os
import re
import json
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup


# ==============================
# OCR Extraction
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
        response = requests.post(url, files=files, data=data, headers=headers)
        if response.status_code != 200:
            print(f"[ERROR] OCR API {response.status_code} for {os.path.basename(image_path)}")
            print(response.text)
            return None

        result = response.json()
        extracted_texts = []
        for page_result in result.get('results', []):
            if page_result.get('success') and page_result.get('message'):
                content = page_result['message']['choices'][0]['message']['content']
                try:
                    parsed_content = json.loads(content)
                    text = parsed_content.get('html', parsed_content.get('natural_text', content))
                except json.JSONDecodeError:
                    text = content
                extracted_texts.append(text)
            else:
                print(f"[WARN] Page error in {os.path.basename(image_path)}: {page_result.get('error')}")
        return '\n'.join(extracted_texts)


# ==============================
# Helpers
# ==============================
THAI_MONTHS = {
    "มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03", "เมษายน": "04",
    "พฤษภาคม": "05", "มิถุนายน": "06", "กรกฎาคม": "07", "สิงหาคม": "08",
    "กันยายน": "09", "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12"
}

def convert_date_round_dd_mm_yyyy(date_str):
    try:
        s = str(date_str).strip()
        if not s or s.lower() == "nan":
            return ""
        parts = s.split(".")
        if len(parts) == 3:
            day, month, year = parts
            day, month, year = int(day), int(month), int(year)
            if year > 2400:
                year -= 543
            return f"{year:04d}-{month:02d}-{day:02d}"
    except Exception:
        pass
    return date_str

def normalize_columns(headers, rows):
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
    if re.search(r'(?i)total', s):
        nums = re.findall(r'\d+(?:\.\d+)?', s)
        return nums[0] if nums else ''
    if re.findall(r'\d+(?:\.\d+)?\s+\d+(?:\.\d+)?', s):
        first = re.findall(r'\d+(?:\.\d+)?', s)
        return first[0] if first else s
    return s


# ==============================
# Non-Table Metadata Parsing
# ==============================
def parse_non_table_metadata(ocr_output: str) -> dict:
    soup = BeautifulSoup(ocr_output, "html.parser")
    for table in soup.find_all("table"):
        table.decompose()
    text_content = soup.get_text(separator="\n")
    lines = [line.strip() for line in text_content.splitlines() if line.strip()]
    if not lines:
        return {}

    text = " ".join(lines)

    topic_match = re.search(r"(รายงานการขายสินค้า\s*-\s*แยกตาม\s*Invoice)", text)
    topic = topic_match.group(1).strip() if topic_match else ""

    date_match = re.search(r"รอบวันที่\s*(\d{1,2})\s*-\s*(\d{1,2})\s*([ก-๙]+)\s*(\d{4})", text)
    if date_match:
        d1, d2, month_th, year = date_match.groups()
        mnum = THAI_MONTHS.get(month_th.strip(), "00")
        start_round_date = f"{int(d1):02d}.{mnum}.{year}"
        end_round_date   = f"{int(d2):02d}.{mnum}.{year}"
    else:
        start_round_date = end_round_date = ""

    vendor_match = re.search(r"#?\s*Vendor\s*(\d+)\s*/\s*([^\(]+)\s*\(?(\d+)?\)?", text)
    if vendor_match:
        number_company = vendor_match.group(1)
        name_company = vendor_match.group(2).strip()
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
# Table Parsing
# ==============================
def parse_tables_to_df(ocr_output: str, pdf_basename: str):
    soup = BeautifulSoup(ocr_output, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        print(f"[WARN] No tables found in {pdf_basename}")
        return None

    dfs = []
    for idx, table in enumerate(tables, start=1):
        first_tr = table.find("tr")
        header_cells = [c.get_text(strip=True) for c in first_tr.find_all(["th", "td"])] if first_tr else []

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
        try:
            df = df.map(lambda x: clean_cell(x))
        except AttributeError:
            df = df.applymap(lambda x: clean_cell(x))

        mask_total = df.astype(str).apply(
            lambda r: r.str.contains(r"(?:\btotal\b|grand\s*total|รวมยอดทั้งหมด)", regex=True, case=False, na=False),
            axis=1
        )
        df = df[~mask_total]
        if "ลำดับที่" in df.columns:
            df = df[df["ลำดับที่"].astype(str).str.strip() != ""]
        dfs.append(df)

    if not dfs:
        print(f"[WARN] No valid table data in {pdf_basename}")
        return None

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.columns = [c.replace("_", ".") for c in final_df.columns]
    final_df = final_df.fillna("")
    return final_df


# ==============================
# Convert DataFrame + Metadata → JSON
# ==============================
def dataframe_to_enriched_rows(df: pd.DataFrame, metadata: dict):
    start_round = convert_date_round_dd_mm_yyyy(metadata.get("start_round_date", ""))
    end_round   = convert_date_round_dd_mm_yyyy(metadata.get("end_round_date", ""))

    out = []
    for _, row in df.iterrows():
        row_dict = {}
        for col, val in row.items():
            col_clean = str(col).strip()
            if col_clean in ["จำนวนเงิน", "ภาษี", "จำนวนเงินสุทธิ"]:
                try:
                    row_dict[col_clean] = round(float(str(val).replace(",", "")), 2)
                except (ValueError, TypeError):
                    row_dict[col_clean] = 0.0
                continue
            if ("วันที่" in col_clean) or ("date" in col_clean.lower()):
                row_dict[col_clean] = convert_date_round_dd_mm_yyyy(val)
                continue
            row_dict[col_clean] = str(val).strip() if str(val).strip() != "" else ""

        row_dict["topic"] = metadata.get("topic", "")
        row_dict["start_round_date"] = start_round
        row_dict["end_round_date"] = end_round
        row_dict["supplier_name"] = metadata.get("supplier_name", "")
        row_dict["supplier_num"] = str(metadata.get("supplier_num", "")).strip()

        out.append(row_dict)
    return out


# ==============================
# Main Processing Loop
# ==============================
def process_pdfs_in_folder(folder_path, api_key, task_type, max_tokens, temperature, top_p, repetition_penalty, output_dir, pages=None):
    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(folder_path):
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
# CLI Entry Point
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Process PDFs with OpenTyphoon OCR and export JSON only.")
    parser.add_argument("--folder", required=True, help="Path to folder containing PDFs.")
    parser.add_argument("--api-key", required=True, help="Your OpenTyphoon API key.")
    parser.add_argument("--task-type", default="structure", help="OCR task type (default: structure).")
    parser.add_argument("--max-tokens", type=int, default=16000)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--top-p", type=float, default=0.6)
    parser.add_argument("--repetition-penalty", type=float, default=1.2)
    parser.add_argument("--pages", type=str, help="Optional JSON list of pages to process.")

    # ✅ Default output dir: processed_data/sale_invoice
    default_output_dir = "processed_data/sale_invoice"
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
