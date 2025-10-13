# services/po_processor.py
import pandas as pd
import os
import datetime

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

def clean_numeric(value):
    if isinstance(value, str):
        value = value.replace(',', '').strip()
        if value == "-" or value == "":
            return 0.0
    try:
        return float(value)
    except:
        return 0.0

def load_po_data(file_path):
    abs_path = os.path.join("raw_data", file_path)
    xls = pd.ExcelFile(abs_path)

    all_data = []
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        if not df.isnull().all().all():  # กัน sheet ว่าง
            df['source_sheet'] = sheet
            all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)

    # ทำความสะอาดชื่อคอลัมน์
    combined_df.columns = [col.strip().lower().replace(" ", "_") for col in combined_df.columns]

    # ระบุคอลัมน์ที่ต้องการเก็บไว้
    keep = [
        "po_no", "po_date", "supplier_name", "buyer_name",
        "delivery_date", "payment_term", "amount_excl_vat",
        "vat_amount", "amount_incl_vat", "source_sheet"
    ]
    combined_df = combined_df[[col for col in keep if col in combined_df.columns]]

    # บังคับให้ po_no เป็น string
    if "po_no" in combined_df.columns:
        combined_df["po_no"] = combined_df["po_no"].fillna('').astype(str)

    # แปลงวันที่
    for date_col in ["po_date", "delivery_date"]:
        if date_col in combined_df.columns:
            combined_df[date_col] = combined_df[date_col].astype(str)
            combined_df[date_col] = combined_df[date_col].apply(fix_buddhist_year)
            combined_df[date_col] = pd.to_datetime(combined_df[date_col], errors="coerce")
            combined_df[date_col] = combined_df[date_col].dt.strftime("%Y-%m-%d")

    # ทำความสะอาดข้อมูลตัวเลข
    for col in ["amount_excl_vat", "vat_amount", "amount_incl_vat"]:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].apply(clean_numeric)

    return combined_df


def save_po_json(dataframe, output_filename):
    os.makedirs("processed_data", exist_ok=True)
    output_path = os.path.join("processed_data", output_filename)
    dataframe.to_json(output_path, orient="records", force_ascii=False, indent=2)
    print(f"JSON saved to {output_path}")
