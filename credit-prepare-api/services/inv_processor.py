# services/inv_processor.py
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

        # อื่น ๆ
        "Supplier Name": "supplier_name",
        "Buyer Name": "buyer_name",
        "Supplier_Name": "supplier_name",
        "Buyer_Name": "buyer_name",
        "PO_No": "po_no",
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
def load_invoice_data(file_path):
    abs_path = os.path.join("raw_data", file_path)
    xls = pd.ExcelFile(abs_path)

    all_data = []
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        if not df.isnull().all().all():
            df['source_sheet'] = sheet
            df = normalize_invoice_columns(df)
            all_data.append(df)

    if not all_data:
        return pd.DataFrame()  # ไม่มีข้อมูลเลย

    combined_df = pd.concat(all_data, ignore_index=True)

    # เลือกเฉพาะคอลัมน์ที่ต้องการ
    keep = [
        "invoice_no", "po_no", "invoice_date", "supplier_name", "buyer_name",
        "amount_excl_vat", "vat_amount", "amount_incl_vat", "source_sheet"
    ]
    combined_df = combined_df[[col for col in keep if col in combined_df.columns]]

    # ✅ invoice_no และ po_no เป็น string เสมอ
    for col in ["invoice_no", "po_no"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna('').astype(str)

    # ✅ แปลง invoice_date เป็น YYYY-MM-DD
    if "invoice_date" in combined_df.columns:
        combined_df["invoice_date"] = combined_df["invoice_date"].astype(str)
        combined_df["invoice_date"] = combined_df["invoice_date"].apply(fix_buddhist_year)
        combined_df["invoice_date"] = pd.to_datetime(combined_df["invoice_date"], errors="coerce")
        combined_df["invoice_date"] = combined_df["invoice_date"].dt.strftime("%Y-%m-%d")

    # ✅ แปลง numeric fields เป็น float
    for col in ["amount_excl_vat", "vat_amount", "amount_incl_vat"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_numeric)

    return combined_df


def save_inv_json(dataframe, output_filename):
    os.makedirs("processed_data", exist_ok=True)
    output_path = os.path.join("processed_data", output_filename)
    dataframe.to_json(output_path, orient="records", force_ascii=False, indent=2)
    print(f"JSON saved to {output_path}")


