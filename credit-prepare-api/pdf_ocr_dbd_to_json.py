#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pdf_ocr_dbd_to_json_v6.py

- Extract text from PDF (prefer pdfminer; fallback Tesseract OCR)
- Save full JSON: <name>.json
- Save structured JSON (DBD table-aware + strong boundaries & tail-noise cleanup): <name>_structured.json
- Merge downloads/<juristic_id>_company_title.json into structured (if present)

pip install pdfminer.six pillow pytesseract pdf2image
# macOS: brew install tesseract poppler
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

TESSERACT_CMD: Optional[str] = None  # set path on Windows if needed


# ---------- PDF text layer ---------- #
def extract_text_pdfminer(pdf_path: str) -> List[str]:
    try:
        from pdfminer_high_level import extract_pages  # type: ignore
        from pdfminer.layout import LTTextContainer  # type: ignore
    except Exception:
        # บางเครื่องใช้ชื่อโมดูลเดิม
        try:
            from pdfminer.high_level import extract_pages  # type: ignore
            from pdfminer.layout import LTTextContainer  # type: ignore
        except Exception:
            return []
    pages = []
    try:
        for layout in extract_pages(pdf_path):
            txt = "".join(el.get_text() for el in layout if isinstance(el, LTTextContainer))
            pages.append(txt.strip())
    except Exception:
        return []
    return pages


# ---------- OCR fallback ---------- #
def ocr_pdf_with_tesseract(pdf_path: str, lang: str = "tha+eng", dpi: int = 300) -> List[str]:
    try:
        from pdf2image import convert_from_path
        import pytesseract
    except Exception as e:
        raise RuntimeError("Need pdf2image/Pillow/pytesseract installed.") from e

    if TESSERACT_CMD:
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

    try:
        images = convert_from_path(pdf_path, dpi=dpi)
    except Exception as e:
        raise RuntimeError("PDF->image failed. Install Poppler & add to PATH.") from e

    out = []
    for img in images:
        out.append(pytesseract.image_to_string(img, lang=lang).strip())
    return out


# ---------- utils ---------- #
def clean_text(s: str) -> str:
    s = s.replace("\r", "\n")
    s = re.sub(r"(\S)\n([่-๋๊-ํ๎])", r"\1\2", s)  # join Thai combining marks split by newline
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class PageResult:
    page: int
    text: str
    lines: List[str]


@dataclass
class OCRResult:
    source_file: str
    file_size_bytes: int
    file_md5: str
    created_at: str
    engine: str
    num_pages: int
    pages: List[PageResult]


# ---------- helpers ---------- #
# Strong boundary: next section / footer/header / noise tokens
BOUNDARY_PAT = re.compile(
    r"(?:^|\s)(?:"
    r"ปีที่ส่งงบการเงิน\s*:|กรรมการ\s*:|คณะกรรมการลงชื่อผูกพัน\s*:|ข้อควรทราบ|"
    r"DBD\s*DataWarehouse|URL\s*:|หน้า\b|ข้อมูล\b|วันที่สั่งพิมพ์\s*:|เวลา\s*:|"
    r"\b\d{1,2}:\d{2}(?::\d{2})?\b|"         # time like 14:59 or 14:59:39
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b|"          # date like 20/10/2025
    r"(?:บริษัท\s+.+?จำกัด)"                 # company footer bleeding in
    r")",
    re.U,
)

TAIL_NOISE_PAT = re.compile(r"[\d\s่-๋๊-ํ๎]+$")  # digits/space/Thai diacritics at tail


def _norm(ln: str) -> str:
    return re.sub(r"\s+", " ", ln).strip(" /")


def _find(full: str, patterns: List[str], flags: int = 0) -> Optional[str]:
    for p in patterns:
        m = re.search(p, full, flags)
        if m:
            return m.group(1).strip()
    return None


def _combine_two_line_key(lines: List[str], i: int) -> Optional[str]:
    cur = _norm(lines[i])
    nxt = _norm(lines[i + 1]) if i + 1 < len(lines) else ""
    if cur == "หมวดธุรกิจ" and re.fullmatch(r"\(มาจากงบการเงินปีล่าสุด\)\s*:", nxt):
        return "หมวดธุรกิจ (มาจากงบการเงินปีล่าสุด) :"
    if cur == "วัตถุประสงค์" and re.fullmatch(r"\(มาจากงบการเงินปีล่าสุด\)\s*:", nxt):
        return "วัตถุประสงค์ (มาจากงบการเงินปีล่าสุด) :"
    return None


def _cut_at_boundaries(value: str) -> str:
    """Trim value at first boundary/time/url/date/company footer; then strip tail noise tokens."""
    # cut at explicit boundaries
    m = BOUNDARY_PAT.search(value)
    if m:
        value = value[: m.start()]

    # stop at inline numbered items (e.g., " 1.ชื่อ")
    m2 = re.search(r"\s\d+\.\s", value)
    if m2:
        value = value[: m2.start()]

    # stop at inline URL
    m3 = re.search(r"https?://\S+", value)
    if m3:
        value = value[: m3.start()]

    # strip trailing pure noise like "1่ ้", "2"
    value = re.sub(TAIL_NOISE_PAT, "", value)
    return value.strip(" /").strip()

def _convert_thai_date_to_iso(date_th: str) -> Optional[str]:
    """แปลงวันที่ไทย DD/MM/YYYY(พ.ศ.) → YYYY-MM-DD (ค.ศ.)"""
    try:
        d, m, y = map(int, date_th.split("/"))
        if y > 2400:  # ถ้าเป็น พ.ศ.
            y -= 543
        return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None


# === directors as objects (helper) ===
def _to_director_objs(names: List[str]) -> List[Dict[str, Any]]:
    """แปลงรายชื่อเป็น [{"no": i, "name": name}] และทำความสะอาด tail '/' + กันซ้ำ"""
    cleaned = []
    seen = set()
    for nm in names:
        nm = (nm or "").strip().rstrip("/").strip()
        if not nm:
            continue
        if nm in seen:
            continue
        seen.add(nm)
        cleaned.append(nm)
    return [{"no": i + 1, "name": nm} for i, nm in enumerate(cleaned)]


def parse_structured_from_pages(pages: List[PageResult]) -> Dict[str, Any]:
    # full text for meta & fallbacks
    full = clean_text("\n".join([p.text for p in pages]))

    # meta
    company_name = _find(full, [r"ข้อมูล\s*\n\s*(บริษัท[^\n]+)", r"^\s*(บริษัท[^\n]+)"], flags=re.M)
    reg_no = _find(full, [r"เลขทะเบียนนิติบุคคล\s*:\s*([0-9\-]+)"])
    entity_type = _find(full, [r"ประเภทนิติบุคคล\s*:\s*([^\n]+)"])
    incorp_date_th = _find(full, [r"วันที่จดทะเบียนจัดตั้ง\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
                                   r"วันที\s*่จดทะเบียนจัดตั\s*้ง\s*:\s*([0-9/]{8,10})"])
    status = _find(full, [r"สถานะนิติบุคคล\s*:\s*([^\n]+)"])
    capital = _find(full, [r"ทุนจดทะเบียน\s*\(บาท\)\s*:\s*([0-9,\.]+)"])
    if capital:
        capital = capital.replace(",", "")

    printed_date = _find(full, [r"วันที่สั่งพิมพ์\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
                                r"วันที\s*่สั\s*่งพิมพ์\s*:\s*([0-9/]{8,10})"])
    printed_time = _find(full, [r"เวลา\s*:\s*([0-9]{2}:[0-9]{2}:[0-9]{2})"])
    source_url = _find(full, [r"URL\s*:\s*(https?://\S+)"])

    # lines
    lines: List[str] = []
    for p in pages:
        for ln in p.lines:
            t = _norm(ln)
            if t:
                lines.append(t)

    ONE_LINE_KEYS = {
        "หมวดธุรกิจตอนจดทะเบียน :",
        "วัตถุประสงค์ตอนจดทะเบียน :",
        "ปีที่ส่งงบการเงิน :",
        "ที่ตั้ง :",
        "กรรมการ :",
        "คณะกรรมการลงชื่อผูกพัน :",
    }

    KEY_TO_FIELD = {
        "หมวดธุรกิจตอนจดทะเบียน :": "business_section_at_registration",
        "วัตถุประสงค์ตอนจดทะเบียน :": "objective_at_registration",
        "หมวดธุรกิจ (มาจากงบการเงินปีล่าสุด) :": "business_section_latest",
        "วัตถุประสงค์ (มาจากงบการเงินปีล่าสุด) :": "objective_latest",
        "ปีที่ส่งงบการเงิน :": "financial_years",
        "ที่ตั้ง :": "address",
        "กรรมการ :": "directors_header",
        "คณะกรรมการลงชื่อผูกพัน :": "binding_rule_header",
    }

    def _key_at(i: int) -> Optional[str]:
        ln = _norm(lines[i])
        for k in ONE_LINE_KEYS:
            if re.fullmatch(re.escape(k).replace(r"\ ", r"\s*"), ln):
                return k
        return _combine_two_line_key(lines, i)

    # --- state machine ---
    results: Dict[str, Any] = {}
    pending: List[str] = []
    buffers: Dict[str, List[str]] = {}

    i = 0
    n = len(lines)
    while i < n:
        k = _key_at(i)
        if k:
            if k == "กรรมการ :":
                j = i + 1
                local = []
                while j < n:
                    if _key_at(j):
                        break
                    txt = _norm(lines[j])
                    if BOUNDARY_PAT.search(txt):
                        break
                    # เก็บเฉพาะรูปแบบ 1. ชื่อ / 2) ชื่อ
                    if re.match(r"^\d+\s*[\.\)]\s*", txt):
                        cand = re.sub(r"^\d+\s*[\.\)]\s*", "", txt).strip(" /-•.")
                        if not any(b in cand for b in ["ข้อมูล", "URL", "หน้า", "DBD", "ปีที่ส่งงบการเงิน"]):
                            local.append(cand)
                    j += 1

                # doc-wide fallback (กันกรณี OCR แทรกเลขไว้ที่อื่น)
                allnums = []
                for x in lines:
                    t = _norm(x)
                    if re.match(r"^\d+\s*[\.\)]\s*", t):
                        cand = re.sub(r"^\d+\s*[\.\)]\s*", "", t).strip(" /-•.")
                        if not any(b in cand for b in ["ข้อมูล", "URL", "หน้า", "DBD", "ปีที่ส่งงบการเงิน"]):
                            allnums.append(cand)

                names, seen = [], set()
                for nm in allnums + local:
                    if nm and nm not in seen:
                        seen.add(nm)
                        names.append(nm)

                results["directors"] = _to_director_objs(names)
                i = j
                continue

            if k == "คณะกรรมการลงชื่อผูกพัน :":
                j = i + 1
                buf = []
                while j < n:
                    if _key_at(j):
                        break
                    txt = _norm(lines[j])
                    if BOUNDARY_PAT.search(txt):
                        break
                    buf.append(txt)
                    j += 1
                s = re.sub(r"\s+", " ", " ".join(buf)).strip(" /")
                s = re.split(r"\sข้อควรทราบ\s*:?", s)[0].strip()
                s = s.replace("คนใดคนหนึ", "คนใดคนหนึ่ง")
                results["binding_rule"] = s
                i = j
                continue

            pending.append(k)
            i += 2 if k in ("หมวดธุรกิจ (มาจากงบการเงินปีล่าสุด) :", "วัตถุประสงค์ (มาจากงบการเงินปีล่าสุด) :") else 1
            continue

        # content for pending keys
        txt = _norm(lines[i])
        if BOUNDARY_PAT.search(txt):
            i += 1
            continue
        if pending:
            if re.match(r"^\d+\s*[\.\)]\s*", txt):
                i += 1
                continue
            tgt = None
            for k2 in pending:
                if k2 not in buffers:
                    tgt = k2
                    buffers[k2] = []
                    break
            if tgt is None:
                tgt = pending[-1]
            buffers[tgt].append(txt)
        i += 1

    def _emit(key: str, val: str):
        field = KEY_TO_FIELD[key]
        val = _cut_at_boundaries(val)
        if field == "financial_years":
            results["financial_years"] = re.findall(r"[0-9]{4}", val)
        elif field == "address":
            results["address"] = val
        elif field in ("business_section_at_registration", "business_section_latest"):
            m = re.search(r"(?P<code>[0-9]{3,5})\s*:\s*(?P<desc>.+)", val)
            if m:
                results[field] = {"code": m.group("code"), "description": m.group("desc")}
            elif val:
                results[field] = {"code": None, "description": val}
        elif field in ("objective_at_registration", "objective_latest"):
            m = re.match(r"(?P<code>[0-9]{3,5})\s*:\s*(?P<desc>.+)", val)
            if m:
                val = m.group("desc")
            results[field] = val

    for k in pending:
        val = " ".join(buffers.get(k, [])).strip()
        if val:
            _emit(k, val)

    # fallbacks
    if "financial_years" not in results:
        yrs = _find(full, [r"ปีที่ส่งงบการเงิน\s*:\s*([0-9\s,]+)"])
        results["financial_years"] = re.findall(r"[0-9]{4}", yrs or "")

    if "directors" not in results:
        allnums = []
        for x in lines:
            t = _norm(x)
            if re.match(r"^\d+\s*[\.\)]\s*", t):
                cand = re.sub(r"^\d+\s*[\.\)]\s*", "", t).strip(" /-•.")
                if not any(b in cand for b in ["ข้อมูล", "URL", "หน้า", "DBD", "ปีที่ส่งงบการเงิน"]):
                    allnums.append(cand)
        results["directors"] = _to_director_objs([d for d in allnums if d])

    out: Dict[str, Any] = {
        "company_name": company_name,
        "registration_number": reg_no,
        "entity_type": entity_type,
        "incorporation_date_th": incorp_date_th,
        "status": status,
        "registered_capital_baht": float(capital) if capital not in (None, "") else None,
        "address": results.get("address"),
        "business_section_at_registration": results.get("business_section_at_registration"),
        "objective_at_registration": results.get("objective_at_registration"),
        "business_section_latest": results.get("business_section_latest"),
        "objective_latest": results.get("objective_latest"),
        "financial_filing_years_th": results.get("financial_years"),
        "directors": results.get("directors"),
        "binding_rule": results.get("binding_rule"),
        "printed_at": {"date": printed_date, "time": printed_time},
        "source_url": source_url,
    }

    if out.get("incorporation_date_th"):
        iso = _convert_thai_date_to_iso(out["incorporation_date_th"])
        if iso:
            out["incorporation_date_th"] = iso

    # ตัดคีย์ที่ค่าว่าง/None
    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


# ---------- NEW: merge company_title.json ---------- #
def merge_company_title(structured: Dict[str, Any], base_dir: str, base_pdf_stem: str) -> Dict[str, Any]:
    """
    รวมข้อมูลจาก <juristic_id>_company_title.json (ถ้ามี) เข้า structured:
    - ใส่คีย์ 'title_card' = เนื้อหาในไฟล์ company_title.json ทั้งก้อน
    - ถ้ามี 'registered_date' ใน title และ structured ยังไม่มี → คัดลอกใส่ที่ root
    - ถ้า structured['address'] ว่าง และ title มี 'head_office_address' → เติม address ให้
    """
    juristic_id = structured.get("registration_number") or base_pdf_stem.split("_")[0]
    title_path = os.path.join(base_dir, f"{juristic_id}_company_title.json")
    if not os.path.isfile(title_path):
        return structured

    try:
        with open(title_path, "r", encoding="utf-8") as f:
            title = json.load(f)
    except Exception:
        return structured

    structured["title_card"] = title

    if title.get("registered_date") and not structured.get("registered_date"):
        structured["registered_date"] = title["registered_date"]

    if title.get("head_office_address") and not structured.get("address"):
        structured["address"] = title["head_office_address"]

    return structured


# ---------- main ---------- #
def main():
    ap = argparse.ArgumentParser(description="DBD OCR to structured JSON (table-aware v6).")
    ap.add_argument("input_pdf")
    ap.add_argument("--lang", default="tha+eng")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--force-ocr", action="store_true")
    ap.add_argument("--structured-only", action="store_true")
    ap.add_argument("--text-only", action="store_true")
    args = ap.parse_args()

    pdf_path = args.input_pdf
    if not os.path.isfile(pdf_path):
        print(f"❌ File not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    base_dir = os.path.dirname(pdf_path)
    base = os.path.splitext(os.path.basename(pdf_path))[0]
    json_full = os.path.join(base_dir, base + ".json")
    json_struct = os.path.join(base_dir, base + "_structured.json")

    pages_text = [] if args.force_ocr else extract_text_pdfminer(pdf_path)
    engine = "pdfminer" if pages_text else "tesseract-ocr"
    if not pages_text:
        pages_text = ocr_pdf_with_tesseract(pdf_path, args.lang, args.dpi)

    pages: List[PageResult] = []
    for i, t in enumerate(pages_text, start=1):
        ct = clean_text(t)
        lines = [ln for ln in ct.splitlines() if ln.strip()]
        pages.append(PageResult(page=i, text=ct, lines=lines))

    if not args.structured_only:
        meta = OCRResult(
            source_file=os.path.abspath(pdf_path),
            file_size_bytes=os.path.getsize(pdf_path),
            file_md5=compute_md5(pdf_path),
            created_at=dt.datetime.now().astimezone().isoformat(),
            engine=engine,
            num_pages=len(pages),
            pages=pages,
        )
        with open(json_full, "w", encoding="utf-8") as f:
            json.dump(asdict(meta), f, ensure_ascii=False, indent=2)
        print(f"Saved full JSON: {json_full}")

    if not args.text_only:
        structured = parse_structured_from_pages(pages)

        # NEW: รวมข้อมูลจาก <juristic_id>_company_title.json ถ้ามี
        structured = merge_company_title(structured, base_dir, base)

        with open(json_struct, "w", encoding="utf-8") as f:
            json.dump(structured, f, ensure_ascii=False, indent=2)
        print(f"Saved structured JSON: {json_struct}")


if __name__ == "__main__":
    main()
