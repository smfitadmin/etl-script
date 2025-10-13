import pandas as pd
import os
import datetime
import re

def fix_buddhist_year(date_val):
    if isinstance(date_val, str):
        if date_val[:4].isdigit():
            year = int(date_val[:4])
            if year > 2400:
                return str(year - 543) + date_val[4:]
        return date_val
    elif isinstance(date_val, pd.Timestamp):
        if date_val.year > 2400:
            return date_val.replace(year=date_val.year - 543)
        return date_val
    elif isinstance(date_val, (int, float)):
        try:
            return pd.to_datetime('1899-12-30') + pd.to_timedelta(date_val, unit='D')
        except Exception:
            return pd.NaT
    elif isinstance(date_val, datetime.datetime):
        if date_val.year > 2400:
            return date_val.replace(year=date_val.year - 543)
        return date_val
    return date_val

def normalize_th_date(val):
    if pd.isna(val):
        return pd.NaT

    s = str(val).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return pd.NaT

    # ให้ตัวคั่นเป็น '-' ทั้งหมด
    s = re.sub(r"[./]", "-", s)

    # กรณีเป็นรูป d-m-y หรือ y-m-d ที่ปลายเป็นปี
    m = re.match(r"^\s*(\d{1,4})-(\d{1,2})-(\d{1,4})\s*$", s)
    if m:
        a, b, c = m.groups()

        # ถ้าเป็นรูปแบบ Y-m-d ให้สลับเป็น d-m-Y เพื่อให้ dayfirst ทำงานตรงตามต้องการ
        if len(a) == 4:  # YYYY-m-d
            y, mth, d = int(a), int(b), int(c)
        else:            # d-m-YYYY หรือ d-m-YY
            d, mth, y = int(a), int(b), int(c)

        # ปี 2 หลัก -> เดาสมเหตุสมผล (00-79 => 2000-2079, 80-99 => 1980-1999 หรือถ้าต้องการ พ.ศ. -> ปรับเอง)
        if y < 100:
            y = 2000 + y  # ปรับตามกติกาที่คุณต้องการ

        # หากเป็นปีพุทธศักราชให้ลบ 543
        if y >= 2400:
            y -= 543

        # สร้างสตริงใหม่เป็น d-m-Y แล้ว parse ด้วย dayfirst
        s = f"{d:02d}-{mth:02d}-{y:04d}"

    # แปลงเป็น datetime (พยายามแบบไทยก่อน dayfirst=True)
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")

    # เผื่อมีกรณีที่ยังหลุด ให้ลอง month-first เป็นทางเลือกสุดท้าย
    if pd.isna(dt):
        dt = pd.to_datetime(s, dayfirst=False, errors="coerce")

    # กรณีสุดท้าย: ถ้ายัง NaT ก็คืน NaT ไป
    return dt

def clean_numeric(value):
    if isinstance(value, str):
        value = value.replace(',', '').strip()
        if value == "-" or value == "":
            return 0.0
    try:
        return float(value)
    except:
        return 0.0
    
def normalize_po_columns(df):
    # จับชื่อคอลัมน์ที่หลากหลายมาแม็ปเป็นชุดเดียว
    rename_map = {
        "Supplier Name": "supplier_name",
        "# Supplier Name": "supplier_name",
        # "Buyer Name": "buyer_name",
        "PO No.": "po_no",
        "PO Date": "po_date",
        "PO Amount (Exclude VAT)": "amount_excl_vat",
        "PO VAT Amount": "vat_amount",
        "PO Net Amount (Include VAT)": "amount_incl_vat",
        "PO Shipment Date": "po_shipment_date",
        "PO Payment Term": "po_payment_term",
    }

    df = df.rename(columns={col: rename_map.get(col, col) for col in df.columns})
    return df

def load_old_po_data(file_path):
    abs_path = file_path if os.path.isabs(file_path) else os.path.join("raw_data", file_path)
    _, ext = os.path.splitext(abs_path)
    ext = ext.lower()
    # abs_path = os.path.join("raw_data", file_path)
    # csv = pd.read_csv(abs_path)

    all_data = []
    if ext in [".xlsx", ".xls"]:
        # Excel หลายชีต
        xls = pd.ExcelFile(abs_path)
        for sheet in xls.sheet_names:
            df = xls.parse(sheet)
            if not df.isnull().all().all():
                df["source_sheet"] = sheet
                df = normalize_po_columns(df)
                all_data.append(df)
    else:
        # CSV มีตารางเดียว
        # เผื่อ encoding ภาษาไทย: ลอง utf-8-sig ก่อน ค่อย fallback cp874
        try:
            df = pd.read_csv(abs_path)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(abs_path, encoding="utf-8-sig")
            except UnicodeDecodeError:
                df = pd.read_csv(abs_path, encoding="cp874")  # Thai Windows
        if not df.isnull().all().all():
            df["source_sheet"] = "CSV"
            df = normalize_po_columns(df)
            all_data.append(df)

    if not all_data:
        return pd.DataFrame()  # ไม่มีข้อมูลเลย

    combined_df = pd.concat(all_data, ignore_index=True)

    # เลือกเฉพาะคอลัมน์ที่ต้องการ (ถ้ามี)
    keep = [
        "supplier_name", 
        "po_no", 
        "po_date", 
        "amount_excl_vat", 
        "vat_amount", 
        "amount_incl_vat",
        "po_shipment_date", 
        "po_payment_term","source_sheet"
    ]
    existing_keep = [col for col in keep if col in combined_df.columns]
    if existing_keep:
        combined_df = combined_df[existing_keep]

    # po_no เป็น string เสมอ
    for col in ["po_no"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].astype(str).fillna("")

    # แปลง invoice_date เป็น YYYY-MM-DD
    if "po_date" in combined_df.columns:
        combined_df["po_date"] = combined_df["po_date"].astype(str)
        combined_df["po_date"] = combined_df["po_date"].apply(normalize_th_date)
        combined_df["po_date"] = pd.to_datetime(combined_df["po_date"], errors="coerce")
        combined_df["po_date"] = combined_df["po_date"].dt.strftime("%Y-%m-%d")

    if "po_shipment_date" in combined_df.columns:
        combined_df["po_shipment_date"] = combined_df["po_shipment_date"].astype(str)
        combined_df["po_shipment_date"] = combined_df["po_shipment_date"].apply(normalize_th_date)
        combined_df["po_shipment_date"] = pd.to_datetime(combined_df["po_shipment_date"], errors="coerce")
        combined_df["po_shipment_date"] = combined_df["po_shipment_date"].dt.strftime("%Y-%m-%d")

    # แปลง numeric fields เป็น float ผ่าน clean_numeric
    for col in ["amount_excl_vat", "vat_amount", "amount_incl_vat"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_numeric)

    return combined_df


def save_old_po_json(dataframe, output_filename):
    os.makedirs("processed_data", exist_ok=True)
    output_path = os.path.join("processed_data", output_filename)
    dataframe.to_json(output_path, orient="records", force_ascii=False, indent=2)
    print(f"JSON saved to {output_path}")
