# filename: pdf_ocr_po_to_json.py
# usage:
#   python pdf_ocr_po_to_json.py po_detail_report_20251003_72195.pdf --method auto
#   python pdf_ocr_po_to_json.py po_detail_report_20250717_2047695.pdf --method table
#   python pdf_ocr_po_to_json.py po_detail_report_20251003_72195.pdf --method ocr

import os, sys, re, json, argparse
from typing import List, Dict, Any, Optional
from datetime import datetime
from tqdm import tqdm

# ----------------- Optional deps -----------------
_HAS_CAMELOT = False
try:
    import camelot
    _HAS_CAMELOT = True
except Exception:
    _HAS_CAMELOT = False

_HAS_TABULA = False
try:
    import tabula
    _HAS_TABULA = True
except Exception:
    _HAS_TABULA = False

_HAS_OCR = False
try:
    from pdf2image import convert_from_path
    import pytesseract
    from PIL import Image
    _HAS_OCR = True
except Exception:
    _HAS_OCR = False

# ----------------- Paths -----------------
INPUT_DIR = os.path.join("raw_data", "po")
OUTPUT_DIR = "processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------- Helpers -----------------
def ensure_poppler_in_path() -> None:
    from shutil import which
    if which("pdfinfo") is None or which("pdftoppm") is None:
        print("[WARN] Poppler not found (pdfinfo/pdftoppm). macOS: `brew install poppler` | Ubuntu: `sudo apt-get install poppler-utils`", file=sys.stderr)

def ensure_tesseract_in_path() -> None:
    from shutil import which
    if which("tesseract") is None:
        print("[WARN] Tesseract not found. macOS: `brew install tesseract` | Ubuntu: `sudo apt-get install tesseract-ocr`", file=sys.stderr)

def to_safe_str(x) -> str:
    return "" if x is None else str(x)

# ----------------- Date/Time Normalization → ISO -----------------
def _strip_am_pm_if_24h(val: str) -> str:
    # ถ้าเป็น 24h แต่ติด AM/PM เช่น '14:54:26 PM' ให้ตัด AM/PM ออก
    m = re.search(r'\b(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)\b', val, re.IGNORECASE)
    if m:
        hour = int(m.group(1))
        if hour >= 13:
            return re.sub(r'\s*(AM|PM)\b', '', val, flags=re.IGNORECASE).strip()
    return val

def _normalize_buddhist_year(s: str) -> str:
    # ปี พ.ศ. ≥ 2400 → ลบ 543 ให้เป็น ค.ศ.
    def repl_dmy(m):
        d, sep1, mth, sep2, y = m.groups()
        yy = int(y)
        if yy >= 2400: yy -= 543
        return f"{d}{sep1}{mth}{sep2}{yy}"
    def repl_ymd(m):
        y, sep1, mth, sep2, d = m.groups()
        yy = int(y)
        if yy >= 2400: yy -= 543
        return f"{yy}{sep1}{mth}{sep2}{d}"
    s = re.sub(r'\b(\d{1,2})([/-])(\d{1,2})([/-])(\d{4})\b', repl_dmy, s)
    s = re.sub(r'\b(\d{4})([/-])(\d{1,2})([/-])(\d{1,2})\b', repl_ymd, s)
    return s

def parse_date_to_iso(val: str) -> Optional[str]:
    """
    คืนค่า 'YYYY-MM-DD'
    รองรับ: dd/mm/yyyy, yyyy-mm-dd, dd-mm-yyyy, yyyy/mm/dd (+ปี พ.ศ.)
    """
    if not val: return None
    v = re.sub(r'\s+', ' ', val.strip())
    v = _normalize_buddhist_year(v)
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(v, fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def parse_datetime_to_iso(val: str) -> Optional[str]:
    """
    คืนค่า 'YYYY-MM-DD HH:MM:SS' (24 ชั่วโมง) ถ้าเจอเวลา
    ถ้าเป็นแค่วันที่ → 'YYYY-MM-DD'
    รองรับ: dd/mm/yyyy hh:mm:ss AM/PM, dd/mm/yyyy hh:mm:ss (24h),
             dd-mm-yyyy, yyyy-mm-dd, yyyy/mm/dd (+ปี พ.ศ.)
    """
    if not val: return None
    v = re.sub(r'\s+', ' ', val.strip())
    v = _normalize_buddhist_year(v)
    v = _strip_am_pm_if_24h(v)

    # datetime ก่อน
    for fmt in (
        "%d/%m/%Y %I:%M:%S %p", "%d-%m-%Y %I:%M:%S %p",  # 12h + AM/PM
        "%d/%m/%Y %H:%M:%S",     "%d-%m-%Y %H:%M:%S",    # 24h
        "%Y-%m-%d %H:%M:%S",     "%Y/%m/%d %H:%M:%S"     # ISO/ymd
    ):
        try:
            d = datetime.strptime(v, fmt)
            return d.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # เหลือเป็น date-only
    iso_d = parse_date_to_iso(v)
    return iso_d

def parse_amount_any(val: Any) -> Optional[float]:
    if val is None: return None
    s = str(val).replace(',', '').strip()
    s = re.sub(r'[^0-9.\-]', '', s)
    if s in ('', '-', '.', '-.'): return None
    try:
        return float(s)
    except ValueError:
        return None

# ----------------- Canonicalize keys -----------------
_CANON_MAP = {
    'no': 'No',
    'pono': 'PO No.',
    'suppliercode': 'Supplier Code',
    'suppliername': 'Supplier Name',
    'orderdate': 'Order Date',
    'senddate': 'Send Date',
    'deliverydate': 'Delivery Date',
    'amount': 'Amount Include VAT',
    'status': 'Status',
}

def _canon_key(k: str) -> str:
    k0 = re.sub(r'\s+', ' ', to_safe_str(k)).strip().replace('\n', ' ')
    key = re.sub(r'[\s._-]+', '', k0.lower())
    return _CANON_MAP.get(key, k0)

def canonicalize_record_keys(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in rec.items():
        if k == "_table_index": 
            continue
        out[_canon_key(k)] = to_safe_str(v).replace('\n', ' ').strip()
    return out

# ----------------- Post-processing record -----------------
def transform_record(record: Dict[str, Any]) -> Dict[str, Any]:
    r = canonicalize_record_keys(record)

    # Order Date → YYYY-MM-DD
    if 'Order Date' in r and r['Order Date']:
        parsed = parse_date_to_iso(r['Order Date'])
        if parsed: r['Order Date'] = parsed

    # Invoice Received Date → YYYY-MM-DD HH:MM:SS (หรือ YYYY-MM-DD ถ้าไม่มีเวลา)
    if 'Send Date' in r and r['Send Date']:
        parsed = parse_datetime_to_iso(r['Send Date'])
        if parsed: r['Send Date'] = parsed

    # Amount → number
    if 'Amount' in r:
        amt = parse_amount_any(r['Amount'])
        if amt is not None:
            r['Amount'] = amt

    # Delivery Date → YYYY-MM-DD
    if 'Delivery Date' in r and r['Delivery Date']:
        parsed = parse_date_to_iso(r['Delivery Date'])
        if parsed: r['Delivery Date'] = parsed

    return r

# ----------------- OCR -----------------
def _preprocess_pil(pil_img: "Image.Image"):
    try:
        import numpy as np, cv2
        img = pil_img.convert("L")
        arr = np.array(img)
        arr = cv2.threshold(arr, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
        arr = cv2.medianBlur(arr, 3)
        from PIL import Image
        return Image.fromarray(arr)
    except Exception:
        return pil_img.convert("L")

def ocr_pdf_to_pages_text(pdf_path: str, dpi: int = 300, lang: str = "tha+eng", tesseract_config: str = "--oem 1 --psm 6") -> List[str]:
    if not _HAS_OCR:
        raise RuntimeError("OCR dependencies missing. Install pdf2image, pytesseract, pillow.")
    ensure_poppler_in_path(); ensure_tesseract_in_path()
    pages = convert_from_path(pdf_path, dpi=dpi)
    texts: List[str] = []
    for p in tqdm(pages, desc="OCR pages"):
        proc = _preprocess_pil(p)
        txt = pytesseract.image_to_string(proc, lang=lang, config=tesseract_config)
        texts.append((txt or "").strip())
    return texts

# ----------------- Table extraction -----------------
def camelot_tables(pdf_path: str) -> Optional[List[Dict[str, Any]]]:
    if not _HAS_CAMELOT:
        return None
    try:
        tb = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
        if tb and tb.n > 0:
            return _camelot_tables_to_records(tb)
        tb = camelot.read_pdf(pdf_path, pages="all", flavor="stream")
        if tb and tb.n > 0:
            return _camelot_tables_to_records(tb)
    except Exception as e:
        print(f"[INFO] Camelot failed: {e}", file=sys.stderr)
    return None

def _camelot_tables_to_records(tables) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for ti, t in enumerate(tables):
        df = t.df
        for r in df.to_dict(orient="records"):
            row = {str(k).strip(): ("" if v is None else str(v).strip()) for k, v in r.items()}
            row["_table_index"] = ti
            recs.append(row)
    return recs

def tabula_tables(pdf_path: str) -> Optional[List[Dict[str, Any]]]:
    if not _HAS_TABULA:
        return None
    try:
        dfs = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True, stream=True)
        all_rows: List[Dict[str, Any]] = []
        for i, df in enumerate(dfs or []):
            df = df.fillna("").astype(str)
            rows = df.to_dict(orient="records")
            for r in rows:
                r["_table_index"] = i
                all_rows.append(r)
        if all_rows:
            return all_rows
    except Exception as e:
        print(f"[INFO] Tabula failed: {e}", file=sys.stderr)
    return None

def normalize_table_records(table_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not table_records:
        return []
    first = table_records[0]
    numeric_keys = all(k.isdigit() for k in first.keys() if k != "_table_index")
    if numeric_keys:
        header_row = first
        headers: List[str] = []
        max_idx = max(int(k) for k in header_row.keys() if k.isdigit()) if header_row else -1
        for idx in range(max_idx + 1):
            raw = header_row.get(str(idx), "")
            h = re.sub(r"\s+", " ", raw).strip()
            headers.append(h)
        norm: List[Dict[str, Any]] = []
        for row in table_records[1:]:
            item: Dict[str, Any] = {}
            for idx, h in enumerate(headers):
                val = row.get(str(idx), "")
                val = re.sub(r"\s+", " ", val).strip()
                item[h] = val
            norm.append(item)
        return norm
    else:
        norm = []
    # Tabula: headers มาแล้ว
        for row in table_records:
            item = {}
            for k, v in row.items():
                if k == "_table_index": continue
                kk = re.sub(r"\s+", " ", str(k)).strip()
                vv = re.sub(r"\s+", " ", to_safe_str(v)).strip()
                item[kk] = vv
            norm.append(item)
        return norm

# ----------------- Orchestrator -----------------
def run_table(pdf_path: str) -> Dict[str, Any]:
    table = camelot_tables(pdf_path)
    mode = "table-camelot"
    if not table:
        table = tabula_tables(pdf_path)
        mode = "table-tabula"
    if not table:
        return {"mode": "table", "records": [], "note": "No table extracted by camelot/tabula."}

    records = normalize_table_records(table)
    records = [transform_record(r) for r in records]
    return {"mode": mode, "records": records}

def run_ocr(pdf_path: str, dpi: int, lang: str) -> Dict[str, Any]:
    pages_text = ocr_pdf_to_pages_text(pdf_path, dpi=dpi, lang=lang)
    return {"mode": "ocr", "pages": [{"page_number": i + 1, "text": t} for i, t in enumerate(pages_text)]}

def run_auto(pdf_path: str, dpi: int, lang: str) -> Dict[str, Any]:
    tbl = run_table(pdf_path)
    if tbl.get("records"):
        return tbl
    return run_ocr(pdf_path, dpi=dpi, lang=lang)

# ----------------- CLI -----------------
def main():
    parser = argparse.ArgumentParser(description="PDF -> JSON (table-first, OCR fallback) with ISO dates.")
    parser.add_argument("filename", help="PDF file name inside raw_data/po")
    parser.add_argument("--method", choices=["auto", "table", "ocr"], default="auto")
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--lang", default="tha+eng")
    parser.add_argument("--input-dir", default=INPUT_DIR)
    parser.add_argument("--out-dir", default=OUTPUT_DIR)
    args = parser.parse_args()

    pdf_path = os.path.join(args.input_dir, args.filename)
    if not os.path.exists(pdf_path):
        print(f"[ERROR] Not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    if args.method == "table":
        doc = run_table(pdf_path)
    elif args.method == "ocr":
        doc = run_ocr(pdf_path, dpi=args.dpi, lang=args.lang)
    else:
        doc = run_auto(pdf_path, dpi=args.dpi, lang=args.lang)

    # อยากได้เฉพาะ array ก็ใช้บรรทัดนี้แทน:
    # out_payload = doc["records"] if "records" in doc else doc
    out_payload = doc

    os.makedirs(args.out_dir, exist_ok=True)
    out_name = os.path.splitext(args.filename)[0] + ".json"
    out_path = os.path.join(args.out_dir, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, ensure_ascii=False, indent=2)
    print(f"[OK] Saved -> {out_path}")

if __name__ == "__main__":
    main()
