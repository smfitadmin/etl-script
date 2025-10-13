import pandas as pd
import os

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
    return date_val

def normalize_invoice_columns(df):
    # จับชื่อคอลัมน์ที่หลากหลายมาแม็ปเป็นชุดเดียว
    rename_map = {
        # แบบเต็ม
        "Invoice Amount (Exclude VAT)": "amount_excl_vat",
        "Invoice VAT Amount": "vat_amount",
        "Invoice Net Amount (Include VAT)": "amount_incl_vat",

        # แบบ snake_case
        "Amount_Excl_VAT": "amount_excl_vat",
        "VAT_Amount": "vat_amount",
        "Amount_Incl_VAT": "amount_incl_vat",

        # แบบมี space
        "Amount Excl. VAT": "amount_excl_vat",
        "VAT Amount": "vat_amount",
        "Amount Incl. VAT": "amount_incl_vat",

        "# Invoice No.": "invoice_no",

        # อื่น ๆ
        "Supplier Code": "supplier_code",
        "Buyer Code": "buyer_code",
        "PO No.": "po_no",
        "PO Date": "po_date",
        "Invoice No.": "invoice_no",
        "Invoice Date": "invoice_date"
    }

    df = df.rename(columns={col: rename_map.get(col, col) for col in df.columns})
    return df

def clean_numeric(value):
    if pd.isnull(value):
        return 0
    if isinstance(value, str):
        value = value.replace(',', '').strip()
        if value in ["", "-", "–"]:
            return 0
    try:
        return float(value)
    except:
        return 0
    
# ฟังก์ชันโหลดข้อมูล invoice จาก Excel
def load_old_invoice_data(file_path):
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
                df = normalize_invoice_columns(df)
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
            df = normalize_invoice_columns(df)
            all_data.append(df)

    if not all_data:
        return pd.DataFrame()  # ไม่มีข้อมูลเลย

    combined_df = pd.concat(all_data, ignore_index=True)

    # เลือกเฉพาะคอลัมน์ที่ต้องการ (ถ้ามี)
    keep = [
        "invoice_no", "invoice_date", "po_no", "po_date", "supplier_code", "buyer_code",
        "amount_excl_vat", "vat_amount", "amount_incl_vat", "source_sheet"
    ]
    existing_keep = [col for col in keep if col in combined_df.columns]
    if existing_keep:
        combined_df = combined_df[existing_keep]

    # invoice_no / po_no / supplier_code / buyer_code เป็น string เสมอ
    for col in ["invoice_no", "po_no", "supplier_code", "buyer_code"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].astype(str).fillna("")

    # แปลง invoice_date เป็น YYYY-MM-DD
    # if "invoice_date" in combined_df.columns:
    #     combined_df["invoice_date"] = combined_df["invoice_date"].astype(str)
    #     combined_df["invoice_date"] = combined_df["invoice_date"].apply(fix_buddhist_year)
    #     combined_df["invoice_date"] = pd.to_datetime(combined_df["invoice_date"], errors="coerce")
    #     combined_df["invoice_date"] = combined_df["invoice_date"].dt.strftime("%Y-%m-%d")

    # แปลง numeric fields เป็น float ผ่าน clean_numeric
    for col in ["amount_excl_vat", "vat_amount", "amount_incl_vat"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_numeric)

    return combined_df


def save_old_inv_json(dataframe, output_filename):
    os.makedirs("processed_data", exist_ok=True)
    output_path = os.path.join("processed_data", output_filename)
    dataframe.to_json(output_path, orient="records", force_ascii=False, indent=2)
    print(f"JSON saved to {output_path}")


