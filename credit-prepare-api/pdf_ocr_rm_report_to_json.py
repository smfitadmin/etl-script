#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdf_ocr_rm_report_to_json.py

วิธีใช้:
    python pdf_ocr_rm_report_to_json.py raw_data/rm/<filename>.pdf

จะสร้างไฟล์:
    processed_data/<ชื่อไฟล์>.json
"""

import argparse
import json
import re
from pathlib import Path

from PyPDF2 import PdfReader


# ---------- helper: date ---------- #

def convert_date(d: str):
    """แปลง DD/MM/YYYY -> YYYY-MM-DD"""
    d = d.strip()
    if not re.match(r"\d{2}/\d{2}/\d{4}", d):
        return None
    day, month, year = d.split("/")
    return f"{year}-{month}-{day}"


# ---------- helper: normalize line ---------- #

def normalize_record_line(line: str) -> str:
    """
    ใส่ช่องว่างรอบ token สำคัญ:
    - ระหว่างเลข 10 หลักกับตัวอักษรถัดไป
    - รอบวันที่, เวลา, จำนวนเงิน
    """
    # space ระหว่างเลข 10 หลักกับตัวอักษรถัดไป (กรณี 0000000000บริษัท)
    line = re.sub(r"^(\d{10})(\S)", r"\1 \2", line)

    # space รอบวันที่
    line = re.sub(r"(\d{2}/\d{2}/\d{4})", r" \1 ", line)

    # space รอบเวลา
    line = re.sub(r"(\d{2}:\d{2}:\d{2}\s+(?:AM|PM))", r" \1 ", line)

    # space รอบจำนวนเงิน
    line = re.sub(r"([0-9,]+\.\d{2})", r" \1 ", line)

    # ลดช่องว่างซ้ำ
    line = re.sub(r"\s+", " ", line).strip()

    return line


# ---------- core parser ---------- #

def parse_remittance_pdf(pdf_path: Path):
    reader = PdfReader(str(pdf_path))
    text = ""

    # ดึง text ทุกหน้า
    for page in reader.pages:
        try:
            t = page.extract_text()
            if t:
                text += t + "\n"
        except:
            pass

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    records = []

    # บรรทัดที่มีวันที่
    has_date_re = re.compile(r"\d{2}/\d{2}/\d{4}")

    # pattern ของ 1 record
    record_re = re.compile(
        r"^(?P<code_10>\d{10})\s+"              # เลข 10 หลักด้านหน้า
        r"(?P<name_branch>.+?)\s+"              # ชื่อ supplier + branch
        r"(?P<date1>\d{2}/\d{2}/\d{4})\s+"      # วันที่ตัวแรก -> Remittance Date
        r"(?P<date2>\d{2}/\d{2}/\d{4})\s+"      # วันที่ตัวที่สอง -> Sent Date
        r"(?P<time>\d{2}:\d{2}:\d{2}\s+(?:AM|PM))\s+"
        r"(?P<amount>[0-9,]+\.\d{2})\s+"
        r"(?P<status>Open|Closed|OPEN|CLOSED|New|NEW)\s+"
        r"(?P<seq>\d+)\s+"
        r"(?P<code_last>\d+)\s+"
        r"(?P<pay_date>\d{2}/\d{2}/\d{4})$"
    )

    i = 0
    n = len(lines)

    while i < n:
        line = lines[i]

        if not re.match(r"^\d{10}", line):
            i += 1
            continue

        # case 1: บรรทัดเดียวครบ
        if has_date_re.search(line):
            record_line = line
        else:
            # case 2: ขึ้นสองบรรทัด (เช่น Banana Society)
            if i + 1 >= n:
                break
            record_line = line + " " + lines[i + 1]
            i += 1

        record_line_norm = normalize_record_line(record_line)
        m = record_re.match(record_line_norm)

        if not m:
            i += 1
            continue

        g = m.groupdict()

        # แยกชื่อ + branch
        name_branch = g["name_branch"].strip()
        parts = name_branch.split()
        if len(parts) >= 2:
            branch = parts[-1]
            supplier_name = " ".join(parts[:-1])
        else:
            branch = ""
            supplier_name = name_branch

        # วันที่
        remittance_date_only = convert_date(g["date1"])
        sent_date_only = convert_date(g["date2"])

        # เวลา
        time_raw = g["time"].strip()       # เช่น 16:18:12 PM
        time_hms = time_raw.split()[0]     # เอาแค่ HH:MM:SS

        # sent_date + time
        sent_datetime = f"{sent_date_only} {time_hms}"

        # mapping ตาม requirement:
        record = {
            "supplier_code": g["code_last"],       # ตัวสุดท้ายก่อน pay_date
            "remittance_no": g["code_10"],         # 10 digit ตัวแรก
            "supplier_name": supplier_name,
            "branch": branch,
            "sent_date": sent_datetime,            # yyyy-mm-dd HH:MM:SS
            "remittance_date": remittance_date_only,
            "amount": float(g["amount"].replace(",", "")),
            "status": g["status"],
            "sequence": int(g["seq"]),
            "pay_date": convert_date(g["pay_date"]),
            "source_pdf": pdf_path.name,
        }

        records.append(record)
        i += 1

    return records


# ---------- main ---------- #

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf_path", help="path ของไฟล์ PDF (input)")
    args = parser.parse_args()

    pdf_path = Path(args.pdf_path)
    if not pdf_path.exists():
        print("ไม่พบไฟล์:", pdf_path)
        return

    out_dir = Path("processed_data")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_json = out_dir / (pdf_path.stem + ".json")

    records = parse_remittance_pdf(pdf_path)
    out_json.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"✔️ แปลงสำเร็จ → {out_json}")


if __name__ == "__main__":
    main()
