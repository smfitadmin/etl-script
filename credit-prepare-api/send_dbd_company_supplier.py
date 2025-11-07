#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
send_dbd_company_supplier.py

โพสต์ไฟล์ JSON ที่มีชื่อ:
  <JURISTIC_ID>_company_info_structured.json
ไปยัง API ด้วย body แบบ JSON (Content-Type: application/json)

ค่าเริ่มต้น API:
  POST http://localhost:8000/api/public/dbd-company-supplier

ตัวอย่างใช้งาน:
  # ส่งไฟล์เดียว
  python send_dbd_company_supplier.py downloads/0105541008416_company_info_structured.json

  # ส่งทั้งโฟลเดอร์
  python send_dbd_company_supplier.py downloads

  # ใช้ glob pattern
  python send_dbd_company_supplier.py "downloads/*_company_info_structured.json"

  # เพิ่มฟิลด์เอง
  python send_dbd_company_supplier.py downloads --extra project=SMF source=dbd

หมายเหตุ:
- จะดึง juristic_id อัตโนมัติจากชื่อไฟล์และแนบเป็นฟิลด์ 'juristic_id'
- ถ้าไฟล์ JSON มี key เดียวกัน จะไม่เขียนทับค่าเดิม
"""

import argparse
import glob
import json
import os
import re
import sys
from typing import Dict, List, Any, Optional

# -------------------------------
# CONFIG / PATTERN
# -------------------------------
JID_FROM_NAME = re.compile(r"^(\d{10,13})_company_info_structured\.json$", re.IGNORECASE)


# -------------------------------
# HELPERS
# -------------------------------
def discover_json_files(input_path: str, default_pattern: str = "*_company_info_structured.json") -> List[str]:
    """ค้นหาไฟล์ JSON ที่ต้องส่ง"""
    if os.path.isfile(input_path) and input_path.lower().endswith(".json"):
        return [input_path]
    if os.path.isdir(input_path):
        return sorted(
            f for f in glob.glob(os.path.join(input_path, default_pattern))
            if f.lower().endswith(".json")
        )
    matches = sorted(glob.glob(input_path))
    return [m for m in matches if m.lower().endswith(".json")]


def extract_jid_from_filename(path: str) -> Optional[str]:
    """ดึงเลขนิติบุคคลจากชื่อไฟล์"""
    base = os.path.basename(path)
    m = JID_FROM_NAME.match(base)
    return m.group(1) if m else None


def parse_kv_pairs(pairs: List[str]) -> Dict[str, str]:
    """แปลง key=value เป็น dict"""
    out: Dict[str, str] = {}
    for p in pairs:
        if "=" not in p:
            raise ValueError(f"Bad --extra pair (expected key=value): {p}")
        k, v = p.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def load_json(path: str) -> Dict[str, Any]:
    """โหลดไฟล์ JSON"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def post_json(
    json_path: str,
    api_url: str,
    timeout: int,
    auto_jid: bool,
    extra_fields: Optional[Dict[str, str]] = None,
) -> bool:
    """ส่งไฟล์ JSON เป็น raw JSON body"""
    try:
        import requests
    except ImportError:
        print("❌ ต้องติดตั้ง requests ก่อน: pip install requests", file=sys.stderr)
        return False

    if not os.path.isfile(json_path):
        print(f"❌ ไม่พบไฟล์: {json_path}", file=sys.stderr)
        return False

    # โหลดเนื้อหา JSON
    try:
        payload = load_json(json_path)
    except Exception as e:
        print(f"❌ โหลด JSON ไม่ได้ ({json_path}): {e}", file=sys.stderr)
        return False

    # แนบ juristic_id จากชื่อไฟล์
    if auto_jid:
        jid = extract_jid_from_filename(json_path)
        if jid and "juristic_id" not in payload:
            payload["juristic_id"] = jid

    # แนบ extra fields
    if extra_fields:
        for k, v in extra_fields.items():
            if k not in payload:
                payload[k] = v

    # แสดง log body (preview)
    preview = json.dumps(payload, ensure_ascii=False, indent=2)
    print(f"📦 Payload for {os.path.basename(json_path)}:\n{preview}\n")

    # ส่งไปยัง API
    try:
        resp = requests.post(api_url, json=payload, timeout=timeout)
        status = resp.status_code
        body_preview = (resp.text or "")[:800]
        if 200 <= status < 300:
            print(f"✅ OK [{status}] {os.path.basename(json_path)} → {api_url}")
            if body_preview:
                print(f"    Response: {body_preview}")
            return True
        else:
            print(f"❌ FAIL [{status}] {os.path.basename(json_path)} → {api_url}", file=sys.stderr)
            if body_preview:
                print(f"    Response: {body_preview}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"❌ ERROR posting {json_path}: {e}", file=sys.stderr)
        return False


# -------------------------------
# MAIN
# -------------------------------
def main():
    ap = argparse.ArgumentParser(description="POST <JID>_company_info_structured.json ไปยัง API (raw JSON body)")
    ap.add_argument("input_path", help="ไฟล์/โฟลเดอร์/หรือ glob pattern ของ *_company_info_structured.json")
    ap.add_argument("--pattern", default="*_company_info_structured.json", help="pattern เมื่อ input เป็นโฟลเดอร์")
    ap.add_argument("--api-url", default="http://localhost:8000/api/public/dbd-company-supplier", help="ปลายทาง API")
    ap.add_argument("--timeout", type=int, default=30, help="timeout วินาที")
    ap.add_argument("--extra", nargs="*", default=[], help="แนบฟิลด์เพิ่มเติม key=value หลายคู่ได้")
    ap.add_argument("--no-auto-jid", action="store_true", help="ไม่ต้องเพิ่ม juristic_id อัตโนมัติจากชื่อไฟล์")

    args = ap.parse_args()

    files = discover_json_files(args.input_path, default_pattern=args.pattern)
    if not files:
        print(f"❌ ไม่พบไฟล์ JSON ที่ตรงกับ {args.input_path}", file=sys.stderr)
        sys.exit(2)

    try:
        extra_fields = parse_kv_pairs(args.extra) if args.extra else {}
    except ValueError as ve:
        print(f"❌ {ve}", file=sys.stderr)
        sys.exit(2)

    print("============================================================")
    print("POST structured JSON → API (raw JSON body)")
    print("============================================================")
    print(f"พบ {len(files)} ไฟล์")
    print(f"API URL : {args.api_url}")
    print(f"Timeout : {args.timeout}s")
    if extra_fields:
        print(f"Extra   : {extra_fields}")
    print("------------------------------------------------------------")

    ok, fail = 0, 0
    for i, fp in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] {fp}")
        success = post_json(
            json_path=fp,
            api_url=args.api_url,
            timeout=args.timeout,
            auto_jid=not args.no_auto_jid,
            extra_fields=extra_fields,
        )
        ok += 1 if success else 0
        fail += 0 if success else 1

    print("------------------------------------------------------------")
    print(f"เสร็จสิ้น ✅  สำเร็จ: {ok}, ล้มเหลว: {fail}")
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
