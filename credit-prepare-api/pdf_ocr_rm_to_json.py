# filename: pdf_ocr_rm_to_json.py
# usage:
#   python pdf_ocr_rm_to_json.py raw_data/rm/xxx.pdf --ocr-mode slow --debug
#   python pdf_ocr_rm_to_json.py processed_data/xxx.json --debug
# output: processed_data/<basename>.json

import os, re, sys, json, argparse, unicodedata, traceback
from typing import List, Dict, Any, Tuple, Optional

PRINT = lambda *a, **k: print(*a, **k, flush=True)

# ---------- Optional OCR deps ----------
_HAS_OCR=False
try:
    from pdf2image import convert_from_path
    import pytesseract
    from PIL import Image
    import numpy as np
    import cv2
    _HAS_OCR=True
except Exception:
    _HAS_OCR=False

OUTPUT_DIR="processed_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- Utils ----------
def to_str(x)->str:
    return "" if x is None else str(x)

def parse_amount(s: Any) -> Optional[float]:
    """
    Robust amount parsing:
    - accepts normal minus '-' and unicode minus '−' (U+2212)
    - accepts parentheses for negative numbers, e.g. (5,463.04)
    - collapses thousands spaces
    """
    if s is None: return None
    if isinstance(s,(int,float)): return float(s)
    t = str(s)
    t = unicodedata.normalize("NFKC", t)
    # Normalize unicode minus to ASCII minus
    t = t.replace("−", "-").replace("—", "-").replace("–", "-")
    # Detect parentheses negative
    neg = False
    m_paren = re.match(r"^\(\s*(.+?)\s*\)$", t)
    if m_paren:
        neg = True
        t = m_paren.group(1)

    # Remove inner spaces between digits (1 171.37 -> 1171.37)
    t = re.sub(r'(?<=\d)\s+(?=\d)', '', t)
    # Keep only digits, dot, comma, and leading minus
    # (we normalized exotic minus to '-')
    t = re.sub(r"[^\d\-,.]", "", t)
    # Remove thousands comma
    t = t.replace(",", "")
    if t in ("", "-", ".", "-."): return None
    try:
        val = float(t)
        if neg: val = -abs(val)
        return val
    except:
        return None

def buddhist_to_ad_date(dmy: str) -> Optional[str]:
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})$", dmy.strip())
    if not m: return None
    d, mth, y = map(int, m.groups())
    if y >= 2400: y -= 543
    return f"{y:04d}-{mth:02d}-{d:02d}"

# ---------- OCR helpers ----------
def _ensure_binaries():
    from shutil import which
    if which("pdftoppm") is None or which("pdfinfo") is None:
        PRINT("[WARN] Poppler not found (pdftoppm/pdfinfo). macOS: brew install poppler")
    if which("tesseract") is None:
        PRINT("[WARN] Tesseract not found. macOS: brew install tesseract")

def _preprocess(img: "Image.Image", mode: str):
    imgs = []
    g = img.convert("L")
    arr = np.array(g)
    if mode == "slow":
        th1 = cv2.threshold(arr, 0,255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)[1]
        th2 = cv2.adaptiveThreshold(arr,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                    cv2.THRESH_BINARY, 31, 5)
        th3 = cv2.bilateralFilter(th2, 7, 50, 50)
        imgs = [Image.fromarray(th1), Image.fromarray(th2), Image.fromarray(th3)]
    else:
        th = cv2.threshold(arr, 0,255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)[1]
        imgs = [Image.fromarray(th)]
    return imgs

def ocr_pdf_to_pages_text(pdf_path: str, mode: str="slow", dpi_slow: int=350, dpi_fast:int=250, debug: bool=False) -> List[Dict[str,Any]]:
    if not _HAS_OCR:
        raise RuntimeError("OCR libraries not installed (pdf2image, pytesseract, pillow, numpy, opencv-python).")
    _ensure_binaries()
    dpi = dpi_slow if mode=="slow" else dpi_fast
    pages = convert_from_path(pdf_path, dpi=dpi)
    out = []
    PRINT(f"[INFO] OCR pages: {len(pages)} (dpi={dpi}, mode={mode})")
    for i, p in enumerate(pages, start=1):
        texts = []
        for v in _preprocess(p, mode):
            txt = pytesseract.image_to_string(v, lang="tha+eng", config="--oem 1 --psm 6")
            t = (txt or "").strip()
            if t and t not in texts:
                texts.append(t)
        merged = "\n".join(texts)
        out.append({"page_number": i, "text": merged})
        if debug:
            PRINT(f"[DEBUG] page {i}: {len(merged.splitlines())} lines (passes={len(texts)})")
    return out

# ---------- Normalizers ----------
def _fix_ocr_o0i1(s: str) -> str:
    if not s: return s
    t = unicodedata.normalize("NFKC", s)
    # ตามกติกา: O->0, I->1 (คง L เป็น L)
    t = t.replace("O","0").replace("I","1")
    return t

def normalize_branch(tok: str) -> str:
    """
    Branch rules:
    - O -> 0, I -> 1 (L stays L)
    - FC + 2 digits  -> FC##  (e.g., 'FC15' -> 'FC15', 'F C 12' -> 'FC12')
    - FC + 1 digit   -> FC0#  (e.g., 'FC1'  -> 'FC01')
    - W + 3 digits   -> W###  (tolerant to stray chars: W 9 0 1 -> W901)
    - '0000' anywhere -> '0000'
    - else return cleaned upper token
    """
    if not tok:
        return tok
    s = _fix_ocr_o0i1(tok).upper()
    s_clean = re.sub(r'[^A-Z0-9 ]', '', s)

    # Explicit 0000
    if '0000' in s_clean:
        return '0000'

    # ---- FC cases ----
    # grab up to first 2 digits after 'FC' (tolerant of spaces/noise)
    m_fc = re.search(r'F\s*C[^0-9]*([0-9])[^0-9]*([0-9])?', s_clean)
    if m_fc:
        d1 = m_fc.group(1)
        d2 = m_fc.group(2)
        if d2 and d2.isdigit():
            return f"FC{d1}{d2}"          # FC + 2 digits  -> FC##
        else:
            return f"FC0{d1}"             # FC + 1 digit   -> FC0#

    # ---- W cases ----
    m_w = re.search(r'W[^0-9]*([0-9])[^0-9]*([0-9])[^0-9]*([0-9])', s_clean)
    if m_w:
        return f"W{m_w.group(1)}{m_w.group(2)}{m_w.group(3)}"

    # fallback: return as-is cleaned
    return s_clean.strip()


def normalize_docref_token(tok: str) -> str:
    """
    ปรับ token ที่เป็นเลขที่เอกสาร / เอกสารอ้างอิง
    - มี SPECIAL RULE สำหรับคำว่า CONSIGN / CONSIGN-00 ที่โดน OCR เพี้ยนเป็น C0NS1GN / C0NS1GN-00
    - กรณีอื่น ๆ ใช้กติกาเดิม: O->0, I->1 และ 1V -> IV
    """
    if not tok:
        return tok

    raw = str(tok).strip()

    # --- SPECIAL RULE สำหรับ CONSIGN / CONSIGN-XX ---
    # OCR มักอ่าน CONSIGN -> C0NS1GN หรือ CONS1GN-00 ฯลฯ
    # pattern นี้ครอบคลุม:
    #   CONSIGN
    #   C0NS1GN
    #   CONSIGN-00 / C0NS1GN-00 / CONS1GN00 etc.
    m = re.match(r"^C[O0]NS[1I]GN(?:[-]?\d\d)?$", raw, re.IGNORECASE)
    if m:
        # ถ้ามีเลขท้าย (เช่น -00 / 00)
        num = re.findall(r"\d\d$", raw)
        if num:
            return f"CONSIGN-{num[0]}"
        return "CONSIGN"

    # --- DEFAULT normalizer เดิม ---
    t = _fix_ocr_o0i1(raw).strip()
    u = t.upper()

    # กรณี 1V -> IV
    if re.match(r'^1V', u):
        t = 'I' + t[1:]

    return t

# เลือก token ที่ “น่าใช่ที่สุด” จากสตริงมั่ว ๆ เช่น "เน?NV68071"
TOKEN_CANDID_RE = re.compile(
    r"[A-Za-z0-9]{2,}[-]?[A-Za-z0-9]{2,}|"
    r"[A-Za-z]{1,3}\s*[-]?\s*\d{4,}",
    re.IGNORECASE
)
def extract_best_token(raw: str) -> Optional[str]:
    if not raw: return None
    t = unicodedata.normalize("NFKC", str(raw))
    cands = TOKEN_CANDID_RE.findall(t)
    if not cands:
        alnum = re.findall(r"[A-Za-z0-9\-]{5,}", t)
        if not alnum: return None
        cand = max(alnum, key=len)
    else:
        cand = max(cands, key=len)
    cand = re.sub(r"\s+", "", cand)
    cand = normalize_docref_token(cand)
    return cand

def repair_ref_if_needed(doc: str, ref: Optional[str]) -> Optional[str]:
    if not ref: 
        return doc
    if not doc: 
        return ref
    common = os.path.commonprefix([doc, ref])
    if len(common) >= 6:
        return doc
    if len(ref) >= 6 and doc.startswith(ref):
        return doc
    return ref

# ---------- Crossdock handling ----------
def _normalize_crossdock_pair(doc_no: str, ref_no: str) -> Tuple[str,str]:
    def fold(x:str)->str:
        if not x: return x
        z = re.sub(r'\s+','',x).upper()
        if z in ('CROSSDOCK','CROSS-DOCK'): return 'CROSS DOCK'
        if z == 'CROSS': return 'CROSS'
        if z == 'DOCK':  return 'DOCK'
        return x
    d1, r1 = fold(doc_no or ''), fold(ref_no or '')
    if re.sub(r'\s+','',d1).upper() in ('CROSSDOCK','CROSS-DOCK'): return ('CROSS DOCK','Crossdock')
    if re.sub(r'\s+','',r1).upper() in ('CROSSDOCK','CROSS-DOCK'): return ('CROSS DOCK','Crossdock')
    du, ru = d1.upper(), r1.upper()
    if (du=='CROSS' and ru=='DOCK') or (du=='DOCK' and ru=='CROSS'): return ('CROSS DOCK','Crossdock')
    if du in ('CROSS','DOCK') and not r1: return ('CROSS DOCK','Crossdock')
    if ru in ('CROSS','DOCK') and not d1: return ('CROSS DOCK','Crossdock')
    return ((doc_no or '').strip(), (ref_no or '').strip())

def normalize_crossdock_in_row(row: Dict[str,Any]) -> Dict[str,Any]:
    d, r = row.get('เลขที่เอกสาร'), row.get('เลขที่เอกสารอ้างอิง')
    nd, nr = _normalize_crossdock_pair(d, r)
    row['เลขที่เอกสาร'] = nd
    row['เลขที่เอกสารอ้างอิง'] = nr
    return row

# ---------- Parsers ----------
DATE_RE = r"(?P<date>\d{2}/\d{2}/\d{4})"
BRANCH_RE = r"(?P<branch>[A-Za-z0-9]{1,8}|0{4})"
TYPE_RE = r"(?P<type>IV|CN)"
DOC_RE = r"(?P<doc>[A-Za-z0-9\-]+|CROSS(?:\s*-?\s*DOCK)?|CROSSDOCK)"
REF_RE = r"(?P<ref>[A-Za-z0-9\-]+|Crossdock|CROSS(?:\s*-?\s*DOCK)?|CROSSDOCK)"

# ✅ รองรับ -, − และ () รอบจำนวนเงิน
AMT_RE = r"(?P<amt>\(?\s*[−-]?[\d\s,]+\.\d{2}\s*\)?)"

LINE_PAT = re.compile(
    rf"{DATE_RE}\s+{BRANCH_RE}\s+{TYPE_RE}\s+{DOC_RE}\s+{REF_RE}\s+{AMT_RE}",
    re.IGNORECASE
)

FALLBACK_PAT = re.compile(
    rf"{DATE_RE}\s+{BRANCH_RE}\s+{TYPE_RE}\s+(\S+)\s+(\S+)\s+{AMT_RE}",
    re.IGNORECASE
)

FALLBACK_ANY_PAT = re.compile(
    rf"{DATE_RE}\s+{BRANCH_RE}\s+{TYPE_RE}\s+(?P<docraw>.+?)\s+(?P<refraw>.+?)\s+{AMT_RE}",
    re.IGNORECASE
)

NO_BRANCH_PAT = re.compile(
    rf"{DATE_RE}\s+{TYPE_RE}\s+(?P<docnb>.+?)\s+(?P<refnb>.+?)\s+{AMT_RE}",
    re.IGNORECASE
)

GRAND_PAT = re.compile(
    r"(?:GRAND\s*TOTAL\s*Amount|จำนวนเงินรวมทั้งสิ้น)\D*?(\(?\s*[−-]?[\d\s,]+\.\d{2}\s*\)?)",
    re.IGNORECASE
)

# header fields
PAYDATE_PAT = re.compile(r"วันที่จ่ายเงิน[:\s]*?(\d{2}/\d{2}/\d{4})")
DOCDATE_PAT = re.compile(r"วันที่เอกสาร[:\s]*?(\d{2}/\d{2}/\d{4})")
VENDOR_PAT  = re.compile(r"รหัสผู้ขาย[:\s]*?([0-9A-Za-z\-]+)")

def parse_page(page: Dict[str,Any], debug: bool=False) -> Dict[str,Any]:
    page_no = int(page.get("page_number", 0))
    text = page.get("text","")
    transactions: List[Dict[str,Any]] = []
    unmatched: List[str] = []
    grands: List[Dict[str,Any]] = []

    pay_date = None
    doc_date = None
    vendor_code = None

    m = PAYDATE_PAT.search(text);  pay_date = buddhist_to_ad_date(m.group(1)) if m else None
    m = DOCDATE_PAT.search(text);  doc_date = buddhist_to_ad_date(m.group(1)) if m else None
    m = VENDOR_PAT.search(text);   vendor_code = _fix_ocr_o0i1(m.group(1)) if m else None

    for gm in GRAND_PAT.finditer(text):
        amt = parse_amount(gm.group(1))
        if amt is not None:
            grands.append({"grand_total_amount": amt, "page": page_no})
            if debug: PRINT(f"[page {page_no}] grand: {amt}")

    for ln in text.splitlines():
        line = ln.strip()
        if not line: continue

        # 1) strict
        m = LINE_PAT.search(line)
        if m:
            gd, gb, gt, gdoc, gref, gamt = (m.group("date"), m.group("branch"), m.group("type"),
                                            m.group("doc"), m.group("ref"), m.group("amt"))
            doc = normalize_docref_token(gdoc)
            ref = normalize_docref_token(gref)
            amt = parse_amount(gamt)
            iso = buddhist_to_ad_date(gd) or gd
            row = {
                "วันที่": iso,
                "วันที่เอกสาร": doc_date,
                "วันที่จ่ายเงิน": pay_date,
                "รหัสผู้ขาย": vendor_code,
                "รหัสสาขา": normalize_branch(gb),
                "ประเภทเอกสาร": gt.upper(),
                "เลขที่เอกสาร": doc,
                "เลขที่เอกสารอ้างอิง": ref,
                "จำนวน": amt,
                "หน้า": page_no
            }
            row = normalize_crossdock_in_row(row)
            transactions.append(row); continue

        # 2) fallback-any
        ma = FALLBACK_ANY_PAT.search(line)
        if ma:
            gd, gb, gt = ma.group("date","branch","type")
            doc_raw, ref_raw = ma.group("docraw"), ma.group("refraw")
            doc_tok = extract_best_token(doc_raw)
            ref_tok = extract_best_token(ref_raw)
            doc_tok = normalize_docref_token(doc_tok) if doc_tok else None
            ref_tok = normalize_docref_token(ref_tok) if ref_tok else None
            ref_tok = repair_ref_if_needed(doc_tok or "", ref_tok)

            amt = parse_amount(ma.group("amt"))
            iso = buddhist_to_ad_date(gd) or gd
            row = {
                "วันที่": iso,
                "วันที่เอกสาร": doc_date,
                "วันที่จ่ายเงิน": pay_date,
                "รหัสผู้ขาย": vendor_code,
                "รหัสสาขา": normalize_branch(gb),
                "ประเภทเอกสาร": gt.upper(),
                "เลขที่เอกสาร": doc_tok or "",
                "เลขที่เอกสารอ้างอิง": ref_tok or "",
                "จำนวน": amt,
                "หน้า": page_no
            }
            row = normalize_crossdock_in_row(row)
            if debug: PRINT(f"[page {page_no}] fallback-any: {gd} {gb} {gt} |{doc_raw}| -> {row['เลขที่เอกสาร']} |{ref_raw}| -> {row['เลขที่เอกสารอ้างอิง']} {ma.group('amt')}")
            transactions.append(row); continue

        # 3) fallback เดิม
        fm = FALLBACK_PAT.search(line)
        if fm:
            gd, gb, gt = fm.group("date","branch","type")
            doc_tok = extract_best_token(fm.group(4))
            ref_tok = extract_best_token(fm.group(5))
            doc_tok = normalize_docref_token(doc_tok) if doc_tok else ""
            ref_tok = normalize_docref_token(ref_tok) if ref_tok else ""
            ref_tok = repair_ref_if_needed(doc_tok, ref_tok)

            amt = parse_amount(fm.group("amt"))
            iso = buddhist_to_ad_date(gd) or gd
            row = {
                "วันที่": iso,
                "วันที่เอกสาร": doc_date,
                "วันที่จ่ายเงิน": pay_date,
                "รหัสผู้ขาย": vendor_code,
                "รหัสสาขา": normalize_branch(gb),
                "ประเภทเอกสาร": gt.upper(),
                "เลขที่เอกสาร": doc_tok,
                "เลขที่เอกสารอ้างอิง": ref_tok,
                "จำนวน": amt,
                "หน้า": page_no
            }
            row = normalize_crossdock_in_row(row)
            if debug: PRINT(f"[page {page_no}] fallback: {gd} {gb} {gt} {doc_tok} {ref_tok} {fm.group('amt')}")
            transactions.append(row); continue

        # 4) ไม่มีรหัสสาขา (เช่น CN หลายแถว)
        nb = NO_BRANCH_PAT.search(line)
        if nb:
            gd, gt = nb.group("date","type")
            doc_tok = extract_best_token(nb.group("docnb"))
            ref_tok = extract_best_token(nb.group("refnb"))
            doc_tok = normalize_docref_token(doc_tok) if doc_tok else ""
            ref_tok = normalize_docref_token(ref_tok) if ref_tok else ""
            ref_tok = repair_ref_if_needed(doc_tok, ref_tok)

            amt = parse_amount(nb.group("amt"))
            iso = buddhist_to_ad_date(gd) or gd
            row = {
                "วันที่": iso,
                "วันที่เอกสาร": doc_date,
                "วันที่จ่ายเงิน": pay_date,
                "รหัสผู้ขาย": vendor_code,
                "รหัสสาขา": "0000",
                "ประเภทเอกสาร": gt.upper(),
                "เลขที่เอกสาร": doc_tok,
                "เลขที่เอกสารอ้างอิง": ref_tok,
                "จำนวน": amt,
                "หน้า": page_no
            }
            row = normalize_crossdock_in_row(row)
            if debug: PRINT(f"[page {page_no}] fallback-no-branch: {gd} {gt} {doc_tok} {ref_tok} {nb.group('amt')}")
            transactions.append(row); continue

        # เก็บไว้ตรวจใน debug
        if re.search(r"\d{2}/\d{2}/\d{4}", line) and re.search(r"[−-]?[\d\s,]+\.\d{2}", line):
            unmatched.append(f"[page {page_no}] {line}")

    return {"transactions": transactions, "grand_totals": grands, "_unmatched": unmatched}

# ---------- Merge & Dedupe ----------
def dedupe_rows(rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    seen = set(); out = []
    for r in rows:
        key = (r.get("วันที่"), r.get("รหัสสาขา"), r.get("ประเภทเอกสาร"),
               r.get("เลขที่เอกสาร"), r.get("เลขที่เอกสารอ้างอิง"),
               float(r.get("จำนวน") if r.get("จำนวน") is not None else 0.0))
        if key in seen: continue
        seen.add(key); out.append(r)
    return out

def dedupe_list_str(items: List[str]) -> List[str]:
    seen=set(); out=[]
    for it in items:
        if it not in seen:
            seen.add(it); out.append(it)
    return out

# ---------- Orchestrators ----------
def read_json_pages(path: str) -> List[Dict[str,Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["pages"] if isinstance(data, dict) and "pages" in data else data

def process_pages(pages: List[Dict[str,Any]], basename: str, debug: bool=False) -> str:
    all_tx: List[Dict[str,Any]] = []
    all_gr: List[Dict[str,Any]] = []
    all_un: List[str] = []

    for p in pages:
        res = parse_page(p, debug=debug)
        all_tx.extend(res["transactions"])
        all_gr.extend(res["grand_totals"])
        all_un.extend(res["_unmatched"])

    all_tx = dedupe_rows(all_tx)
    if debug: all_un = dedupe_list_str(all_un)

    out = { "file": basename, "grand_totals": all_gr, "transactions": all_tx }
    if debug: out["_unmatched"] = all_un

    out_path = os.path.join(OUTPUT_DIR, os.path.splitext(os.path.basename(basename))[0] + ".json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    PRINT(f"[OK] Saved -> {out_path} (tx={len(all_tx)}, grand={len(all_gr)}{', unmatched='+str(len(all_un)) if debug else ''})")
    return out_path

# ---------- CLI ----------
def main():
    PRINT(f"[RUN] {os.path.basename(__file__)} cwd={os.getcwd()}")
    ap = argparse.ArgumentParser(description="Extract CPALL Remittance (transactions + grand totals with page) from PDF or JSON(pages).")
    ap.add_argument("input_path", help="PDF path or JSON path (pages).")
    ap.add_argument("--ocr-mode", choices=["slow","fast"], default="slow", help="OCR quality/performance mode (PDF only).")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    src = args.input_path
    if not os.path.exists(src):
        PRINT(f"[ERROR] Not found: {src}")
        sys.exit(1)

    ext = os.path.splitext(src)[1].lower()
    basename = os.path.basename(src)

    try:
        if ext == ".pdf":
            if not _HAS_OCR:
                PRINT("[ERROR] OCR libraries not installed; install pdf2image, pytesseract, pillow, numpy, opencv-python")
                sys.exit(1)
            PRINT(f"[INFO] Input is PDF: {basename}, mode={args.ocr_mode}")
            pages = ocr_pdf_to_pages_text(src, mode=args.ocr_mode, debug=args.debug)
            PRINT(f"[INFO] Parsed pages: {len(pages)} → extracting…")
            process_pages(pages, basename, debug=args.debug)
        else:
            PRINT(f"[INFO] Input is JSON pages: {basename}")
            pages = read_json_pages(src)
            PRINT(f"[INFO] Loaded pages: {len(pages)} → extracting…")
            process_pages(pages, basename, debug=args.debug)
    except Exception:
        PRINT("[FATAL] Exception occurred:")
        PRINT(traceback.format_exc())
        sys.exit(2)

if __name__ == "__main__":
    main()
