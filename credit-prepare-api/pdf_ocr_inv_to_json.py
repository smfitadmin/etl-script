# filename: pdf_ocr_to_json.py
# usage ตัวอย่าง:
#   python pdf_ocr_to_json.py invoice_detail_report_20251003_72195.pdf --method table --engine tabula --records-only
#   # ถ้าข้อมูล OCR เพี้ยนตัวที่คล้ายเลข ให้ช่วยซ่อมเฉพาะส่วนตัวเลขท้าย: เพิ่ม --fix-lookalikes
#   python pdf_ocr_to_json.py invoice_detail_report_20251003_72195.pdf --method table --engine tabula --records-only --fix-lookalikes
#   # ถ้าอยากคัดเข้ม (filter): เพิ่ม --strict

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
INPUT_DIR = os.path.join("raw_data", "inv")
OUTPUT_DIR = os.path.join("processed_data", "inv")
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

def norm_ws(x: str) -> str:
    return re.sub(r"\s+", " ", x.replace("\n", " ")).strip()

# ----------------- Date/Time Normalization -----------------
def _strip_am_pm_if_24h(val: str) -> str:
    m = re.search(r'\b(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)\b', val, re.IGNORECASE)
    if m and int(m.group(1)) >= 13:
        return re.sub(r'\s*(AM|PM)\b', '', val, flags=re.IGNORECASE).strip()
    return val

def _normalize_buddhist_year(s: str) -> str:
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
    if not val: return None
    v = norm_ws(val)
    v = _normalize_buddhist_year(v)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            d = datetime.strptime(v, fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def parse_date_mmdd_to_iso(val: str) -> Optional[str]:
    if not val: return None
    v = norm_ws(val)
    v = _normalize_buddhist_year(v)
    for fmt in ("%m/%d/%Y", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", v)
    if m:
        mo, day, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100: y += 2000 if y < 50 else 1900
        try:
            return datetime(y, mo, day).strftime("%Y-%m-%d")
        except Exception:
            return None
    return None

def parse_datetime_to_iso(val: str) -> Optional[str]:
    if not val: return None
    v = norm_ws(val)
    v = _normalize_buddhist_year(v)
    v = _strip_am_pm_if_24h(v)
    for fmt in (
        "%d/%m/%Y %I:%M:%S %p", "%d-%m-%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p", "%m-%d-%Y %I:%M:%S %p",
        "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"
    ):
        try:
            d = datetime.strptime(v, fmt)
            return d.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    iso = parse_date_to_iso(v) or parse_date_mmdd_to_iso(v)
    return iso

def parse_amount_any(val: Any) -> Optional[float]:
    if val is None: return None
    s = re.sub(r'[^0-9.\-]', '', str(val).replace(',', '').strip())
    if s in ('', '-', '.', '-.'): return None
    try: return float(s)
    except ValueError: return None

# ----------------- Canonicalize keys -----------------
_CANON_MAP = {
    'no': 'No',
    'invoice#': 'Invoice No.',
    'invoiceno': 'Invoice No.',
    'invoicenumber': 'Invoice No.',
    'taxinvoiceno': 'Invoice No.',
    'suppliercode': 'Supplier Code',
    'suppliername': 'Supplier Name',
    'invoicedate': 'Invoice Date',
    'invoicereceiveddate': 'Invoice Received Date',
    'receiveddate': 'Invoice Received Date',
    'relateddocument': 'Related Document',
    'po': 'Related Document',
    'amount': 'Amount',
    'amountincvat': 'Amount',
    'amount(includevat)': 'Amount',
    'status': 'Status',
}

def _canon_key(k: str) -> str:
    k0 = norm_ws(to_safe_str(k))
    key = re.sub(r'[\s._\-:()]+', '', k0.lower())
    return _CANON_MAP.get(key, k0)

def canonicalize_record_keys(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in rec.items():
        if k == "_table_index": continue
        out[_canon_key(k)] = norm_ws(to_safe_str(v))
    return out

# ----------------- Header/tail clean -----------------
_EXPECTED_HEADER_LABELS = {
    "No", "Invoice No.", "Supplier Code", "Supplier Name",
    "Invoice Date", "Invoice Received Date", "Related Document",
    "Amount", "Status"
}
_TOTAL_PAT = re.compile(r"\b(total|grand\s*total|sub\s*total)\b", re.IGNORECASE)
_THAI_TOTAL_SUBSTRS = ("รวมทั้งสิ้น", "ยอดรวม", "รวม")

def _rec_is_empty(rec: Dict[str, Any]) -> bool:
    return all((v is None) or (isinstance(v, str) and v.strip() == "") for v in rec.values())

def _rec_has_total(rec: Dict[str, Any]) -> bool:
    for v in rec.values():
        if isinstance(v, str):
            t = v.strip().lower()
            if _TOTAL_PAT.search(t): return True
            if any(s in t for s in _THAI_TOTAL_SUBSTRS): return True
    return False

def _rec_looks_like_header(rec: Dict[str, Any]) -> bool:
    vals = set(norm_ws(v) for v in rec.values() if isinstance(v, str))
    return len(_EXPECTED_HEADER_LABELS.intersection(vals)) >= 3

# ----------------- Invoice No. lookalikes fixer (optional) -----------------
_INVOICE_PREFIX = re.compile(r'^([A-Za-z]+)(.*)$')  # prefix letters + tail
def normalize_invoice_no_tail_digits(val: str) -> str:
    """
    ซ่อมเฉพาะ 'ส่วนท้าย' ที่เป็นตัวเลขหลัง prefix ตัวอักษร:
      - l/L/i/I -> 1,  O/o -> 0   (เฉพาะใน tail)
    ไม่แตะ prefix (เช่น BL, CHO, MHC, NS, SU ... )
    """
    if not val:
        return val
    s = norm_ws(val).replace(" ", "")
    m = _INVOICE_PREFIX.match(s)
    if not m:
        return s
    prefix, tail = m.group(1), m.group(2)
    # แทนที่ lookalikes ใน tail เท่านั้น
    tail_fixed = []
    for ch in tail:
        if ch in "lLiI":
            tail_fixed.append("1")
        elif ch in "oO":
            tail_fixed.append("0")
        else:
            tail_fixed.append(ch)
    return prefix.upper() + "".join(tail_fixed)

# ----------------- Related Document fixer -----------------
def fix_related_document(val: str) -> str:
    """ดึงเลขท้าย 8–14 หลักจากสตริง เช่น 'PO:1013090869' -> '1013090869'"""
    if not val: return ""
    m = re.search(r'(\d{8,14})\b', val.replace(' ', ''))
    return m.group(1) if m else val

# ----------------- Record transformers -----------------
# strict validators (ใช้เมื่อ --strict)
_RE_INVNO_STRICT = re.compile(r'^[A-Za-z]+\d+$')
_RE_SUPCODE = re.compile(r'^\d+$')

def transform_record_lenient(rec: Dict[str, Any], fix_lookalikes: bool) -> Optional[Dict[str, Any]]:
    """
    โหมด 'lenient' (ดีฟอลต์): เก็บทุกแถวที่ไม่ใช่หัวคอลัมน์/แถวว่าง
    - ซ่อม Invoice No. เฉพาะกรณีสั่ง --fix-lookalikes (แก้เฉพาะ tail)
    - ซ่อม Related Document, วันเวลา, Amount
    """
    r = canonicalize_record_keys(rec)
    if _rec_looks_like_header(r) or r.get("No", "").lower() == "no":
        return None

    if "Invoice No." in r and r["Invoice No."] and fix_lookalikes:
        r["Invoice No."] = normalize_invoice_no_tail_digits(r["Invoice No."])

    if "Related Document" in r:
        r["Related Document"] = fix_related_document(r["Related Document"])

    if "Invoice Date" in r and r["Invoice Date"]:
        r["Invoice Date"] = parse_date_to_iso(r["Invoice Date"]) or parse_date_mmdd_to_iso(r["Invoice Date"]) or r["Invoice Date"]

    if "Invoice Received Date" in r and r["Invoice Received Date"]:
        r["Invoice Received Date"] = parse_datetime_to_iso(r["Invoice Received Date"]) or r["Invoice Received Date"]

    if "Amount" in r:
        amt = parse_amount_any(r["Amount"])
        if amt is not None:
            r["Amount"] = amt

    return None if _rec_is_empty(r) else r

def transform_record_strict(rec: Dict[str, Any], fix_lookalikes: bool) -> Optional[Dict[str, Any]]:
    """
    โหมด 'strict': ต้องเป็น 'ตัวอักษร+ตัวเลข' สำหรับ Invoice No. และ Supplier Code เป็นตัวเลขล้วน
    """
    r = canonicalize_record_keys(rec)
    if _rec_looks_like_header(r) or r.get("No", "").lower() == "no":
        return None

    invno = r.get("Invoice No.", "")
    if invno and fix_lookalikes:
        invno = normalize_invoice_no_tail_digits(invno)
        r["Invoice No."] = invno

    if not _RE_INVNO_STRICT.match(invno or ""):
        return None
    if not _RE_SUPCODE.match(r.get("Supplier Code", "")):
        return None

    if "Related Document" in r:
        r["Related Document"] = fix_related_document(r["Related Document"])

    if "Invoice Date" in r and r["Invoice Date"]:
        r["Invoice Date"] = parse_date_to_iso(r["Invoice Date"]) or parse_date_mmdd_to_iso(r["Invoice Date"]) or r["Invoice Date"]

    if "Invoice Received Date" in r and r["Invoice Received Date"]:
        r["Invoice Received Date"] = parse_datetime_to_iso(r["Invoice Received Date"]) or r["Invoice Received Date"]

    if "Amount" in r:
        amt = parse_amount_any(r["Amount"])
        if amt is not None:
            r["Amount"] = amt

    return None if _rec_is_empty(r) else r

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

def ocr_pdf_to_pages_text(pdf_path: str, dpi: int = 300, lang: str = "tha+eng", tesseract_config: str = "--oem 1 --psm 6") -> Dict[str, Any]:
    if not _HAS_OCR:
        raise RuntimeError("OCR dependencies missing. Install pdf2image, pytesseract, pillow.")
    ensure_poppler_in_path(); ensure_tesseract_in_path()
    pages = convert_from_path(pdf_path, dpi=dpi)
    texts: List[str] = []
    for p in tqdm(pages, desc="OCR pages"):
        proc = _preprocess_pil(p)
        txt = pytesseract.image_to_string(proc, lang=lang, config=tesseract_config)
        texts.append((txt or "").strip())
    return {"mode": "ocr", "pages": [{"page_number": i + 1, "text": t} for i, t in enumerate(texts)]}

# ----------------- Table extraction -----------------
def _camelot_tables_to_records(tables) -> List[Dict[str, Any]]:
    recs: List[Dict[str, Any]] = []
    for ti, t in enumerate(tables):
        df = t.df
        for r in df.to_dict(orient="records"):
            row = {str(k).strip(): ("" if v is None else str(v).strip()) for k, v in r.items()}
            row["_table_index"] = ti
            recs.append(row)
    return recs

def camelot_tables(pdf_path: str, flavor: str = "lattice") -> Optional[List[Dict[str, Any]]]:
    if not _HAS_CAMELOT:
        return None
    try:
        tb = camelot.read_pdf(pdf_path, pages="all", flavor=flavor)
        if tb and tb.n > 0:
            return _camelot_tables_to_records(tb)
    except Exception as e:
        print(f"[INFO] Camelot ({flavor}) failed: {e}", file=sys.stderr)
    return None

def tabula_tables(pdf_path: str) -> Optional[List[Dict[str, Any]]]:
    if not _HAS_TABULA:
        return None
    try:
        dfs = tabula.read_pdf(pdf_path, pages="all", multiple_tables=True, stream=True)
        rows: List[Dict[str, Any]] = []
        for i, df in enumerate(dfs or []):
            df = df.fillna("").astype(str)
            for r in df.to_dict(orient="records"):
                r["_table_index"] = i
                rows.append(r)
        return rows or None
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
            headers.append(norm_ws(header_row.get(str(idx), "")))
        norm: List[Dict[str, Any]] = []
        for row in table_records[1:]:
            item = {headers[i]: norm_ws(row.get(str(i), "")) for i in range(len(headers))}
            norm.append(item)
        return norm
    else:
        norm = []
        for row in table_records:
            item = {}
            for k, v in row.items():
                if k == "_table_index": continue
                item[norm_ws(str(k))] = norm_ws(to_safe_str(v))
            norm.append(item)
        return norm

# ----------------- Orchestrators -----------------
def run_table(pdf_path: str, engine: str = "auto", strict: bool = False, fix_lookalikes: bool = False) -> Dict[str, Any]:
    rows = None
    mode = None

    if engine in ("auto", "camelot-lattice"):
        rows = camelot_tables(pdf_path, flavor="lattice"); mode = "table-camelot-lattice" if rows else mode
    if rows is None and engine in ("auto", "camelot-stream"):
        rows = camelot_tables(pdf_path, flavor="stream");  mode = "table-camelot-stream" if rows else mode
    if rows is None and engine in ("auto", "tabula"):
        rows = tabula_tables(pdf_path);                    mode = "table-tabula" if rows else mode

    if not rows:
        return {"mode": "table", "records": [], "note": f"No table extracted (engine={engine})."}

    records = normalize_table_records(rows)

    # แปลง + กรอง
    fixed: List[Dict[str, Any]] = []
    transformer = (lambda rec: transform_record_strict(rec, fix_lookalikes)) if strict \
                  else (lambda rec: transform_record_lenient(rec, fix_lookalikes))
    for r in records:
        rr = transformer(r)
        if rr is not None:
            fixed.append(rr)

    # ลบแถวท้ายที่เป็น Total/ว่าง
    end = len(fixed)
    while end > 0:
        if _rec_is_empty(fixed[end-1]) or _rec_has_total(fixed[end-1]):
            end -= 1
        else:
            break
    fixed = fixed[:end]

    return {"mode": mode or "table", "records": fixed}

def run_ocr(pdf_path: str, dpi: int, lang: str) -> Dict[str, Any]:
    return ocr_pdf_to_pages_text(pdf_path, dpi=dpi, lang=lang)

def run_auto(pdf_path: str, dpi: int, lang: str, engine: str, strict: bool, fix_lookalikes: bool) -> Dict[str, Any]:
    tbl = run_table(pdf_path, engine=engine, strict=strict, fix_lookalikes=fix_lookalikes)
    if tbl.get("records"):
        return tbl
    return run_ocr(pdf_path, dpi=dpi, lang=lang)

# ----------------- CLI -----------------
def main():
    p = argparse.ArgumentParser(description="PDF -> JSON (table-first, OCR fallback) | keep all rows by default.")
    p.add_argument("filename", help="PDF file name inside raw_data/inv")
    p.add_argument("--method", choices=["auto", "table", "ocr"], default="auto")
    p.add_argument("--engine", choices=["auto", "tabula", "camelot-lattice", "camelot-stream"], default="auto",
                   help="Table extraction engine preference (suggest: tabula).")
    p.add_argument("--dpi", type=int, default=300)
    p.add_argument("--lang", default="tha+eng")
    p.add_argument("--input-dir", default=INPUT_DIR)
    p.add_argument("--out-dir", default=OUTPUT_DIR)
    p.add_argument("--records-only", action="store_true", help="Export only the array of records (if available).")
    p.add_argument("--sort-by", default=None, help="Column name to sort by (e.g. 'Invoice Date').")
    p.add_argument("--sort-desc", action="store_true", help="Sort descending.")
    p.add_argument("--strict", action="store_true", help="Enable strict validation (filters rows by patterns).")
    p.add_argument("--fix-lookalikes", action="store_true", help="Fix lookalike characters in the numeric tail of 'Invoice No.' (l/I -> 1, o/O -> 0).")
    args = p.parse_args()

    pdf_path = os.path.join(args.input_dir, args.filename)
    if not os.path.exists(pdf_path):
        print(f"[ERROR] Not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    if args.method == "table":
        doc = run_table(pdf_path, engine=args.engine, strict=args.strict, fix_lookalikes=args.fix_lookalikes)
    elif args.method == "ocr":
        doc = run_ocr(pdf_path, dpi=args.dpi, lang=args.lang)
    else:
        doc = run_auto(pdf_path, dpi=args.dpi, lang=args.lang, engine=args.engine, strict=args.strict, fix_lookalikes=args.fix_lookalikes)

    # optional sort
    if args.sort_by and "records" in doc:
        try:
            key = args.sort_by
            def _k(r):
                v = r.get(key)
                if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", v):
                    return v
                return v if v is not None else ""
            doc["records"] = sorted(doc["records"], key=_k, reverse=bool(args.sort_desc))
        except Exception as e:
            print(f"[WARN] sort failed: {e}", file=sys.stderr)

    # output
    payload = doc["records"] if (args.records_only and "records" in doc) else doc
    os.makedirs(args.out_dir, exist_ok=True)
    out_name = os.path.splitext(args.filename)[0] + ".json"
    out_path = os.path.join(args.out_dir, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[OK] Saved -> {out_path} (mode: {doc.get('mode')}, strict={args.strict}, fix_lookalikes={args.fix_lookalikes})")

if __name__ == "__main__":
    main()
