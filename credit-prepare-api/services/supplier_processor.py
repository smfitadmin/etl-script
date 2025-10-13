import pandas as pd
import os
from datetime import datetime, timedelta

def rename_thai_columns(df):
    column_map = {
        "ทะเบียนนิติบุคคล": "registration_id",
        "Supplier ID": "supplier_id",
        "IsSupplier": "is_supplier",
        "Start Effective Date": "start_effective_date",
        "Size": "size",
        "Supplier Name": "supplier_name",
        "วันที่จดทะเบียน": "registration_date",
        "ทุนจดทะเบียน": "registered_capital",
        "ลูกหนี้การค้าสุทธิ": "trade_receivables_net",
        "สินค้าคงเหลือ": "inventory",
        "สินทรัพย์หมุนเวียน": "current_assets",
        "ที่ดิน อาคารและอุปกรณ์": "property_plant_equipment",
        "สินทรัพย์ไม่หมุนเวียน": "non_current_assets",
        "สินทรัพย์รวม": "total_assets",
        "หนี้สินหมุนเวียน": "current_liabilities",
        "หนี้สินไม่หมุนเวียน": "non_current_liabilities",
        "หนี้สินรวม": "total_liabilities",
        "ส่วนของผู้ถือหุ้น": "shareholders_equity",
        "หนี้สินรวมและส่วนของผู้ถือหุ้น": "liabilities_and_equity",
        "Group": "group",
        "รายได้หลัก": "main_revenue",
        "รายได้รวมตามงบการเงิน": "total_revenue_fs",
        "ต้นทุนขาย": "cost_of_goods_sold",
        "กำไร(ขาดทุน) ขั้นต้น": "gross_profit",
        "ค่าใช้จ่ายในการขายและบริการ": "selling_and_admin_expenses",
        "รายจ่ายรวม": "total_expenses",
        "ดอกเบี้ยจ่าย": "interest_expense",
        "กำไร(ขาดทุน) ก่อนภาษี": "profit_before_tax",
        "ภาษีเงินได้": "income_tax",
        "กำไร(ขาดทุน)สุทธิ": "net_profit",
        "No of Buyer": "no_of_buyer",
        "อัตราผลตอบแทนจากสินทรัพย์รวม(ROA)(%)": "roa_percent",
        "อัตราผลตอบแทนจากส่วนของผู้ถือหุ้น(ROE)(%)": "roe_percent",
        "ผลตอบแทนจากกำไรขั้นต้นต่อรายได้รวม(%)": "gross_profit_margin_percent",
        "ผลตอบแทนจากการดำเนินงานต่อรายได้รวม(%)": "operating_margin_percent",
        "ผลตอบแทนจากกำไรสุทธิต่อรายได้รวม(%)": "net_margin_percent",
        "อัตราหมุนเวียนของสินทรัพย์รวม(เท่า)": "asset_turnover_ratio",
        "อัตราหมุนเวียนของลูกหนี้(เท่า)": "receivables_turnover_ratio",
        "อัตราหมุนเวียนของสินค้าคงเหลือ(เท่า)": "inventory_turnover_ratio",
        "อัตราค่าใช้จ่ายดำเนินงานต่อรายได้รวม (%)": "operating_expense_ratio",
        "อัตราส่วนทุนหมุนเวียน(เท่า)": "current_ratio",
        "อัตราส่วนหนี้สินรวมต่อสินทรัพย์รวม(เท่า)": "debt_to_asset_ratio",
        "อัตราส่วนสินทรัพย์รวมต่อส่วนของผู้ถือหุ้น(เท่า)": "asset_to_equity_ratio",
        "อัตราส่วนหนี้สินรวมต่อส่วนของผู้ถือหุ้น(เท่า)": "debt_to_equity_ratio"
    }
    return df.rename(columns=column_map)


def fix_buddhist_year(date_val):
    try:
        if isinstance(date_val, (int, float)):
            if 0 < date_val <= 60000:
                return datetime(1899, 12, 30) + timedelta(days=int(date_val))
            else:
                return pd.NaT

        if isinstance(date_val, str):
            date_val = date_val.strip()
            if "/" in date_val:
                parts = date_val.split("/")
                if len(parts) == 3:
                    day, month, year = parts
                    if day.isdigit() and month.isdigit() and year.isdigit():
                        day = int(day)
                        month = int(month)
                        year = int(year)
                        if year > 2500:
                            year -= 543
                        return datetime(year, month, day)
            elif date_val.isdigit():
                val = float(date_val)
                if 0 < val <= 60000:
                    return datetime(1899, 12, 30) + timedelta(days=int(val))
                else:
                    return pd.NaT

        if isinstance(date_val, (datetime, pd.Timestamp)):
            return date_val

    except Exception:
        return pd.NaT

    return pd.NaT

def excel_serial_to_date(serial) -> str:
    try:
        if isinstance(serial, (int, float)):
            val = float(serial)
            base_date = datetime(1899, 12, 30)
            date = base_date + timedelta(days=int(val))
            return f"{date.year - 543}-{date.month:02d}-{date.day:02d}"
        
        if isinstance(serial, str):
            val = serial.strip()
            if "/" in val:
                parts = val.split("/")
                if len(parts) == 3:
                    day, month, year = parts
                    if day.isdigit() and month.isdigit() and year.isdigit():
                        day = int(day)
                        month = int(month)
                        year = int(year)
                        if year > 2500:
                            year -= 543
                        date = datetime(year, month, day)
                        return f"{date.year}-{date.month:02d}-{date.day:02d}"

        # datetime
        if isinstance(serial, (datetime, pd.Timestamp)):
            return f"{serial.year}-{serial.month:02d}-{serial.day:02d}"

    except:
        return None


def load_supplier_data(file_path):
    abs_path = os.path.join("raw_data", file_path)
    xls = pd.ExcelFile(abs_path)
    all_data = []

    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        if not df.isnull().all().all():
            df["source_sheet"] = sheet
            all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = rename_thai_columns(combined_df)

    if "registration_id" in combined_df.columns:
        combined_df["registration_id"] = combined_df["registration_id"].astype(str)

    if "start_effective_date" in combined_df.columns:
        combined_df["start_effective_date"] = combined_df["start_effective_date"].apply(fix_buddhist_year)
        combined_df["start_effective_date"] = pd.to_datetime(combined_df["start_effective_date"], errors="coerce")
        combined_df["start_effective_date"] = combined_df["start_effective_date"].dt.strftime("%Y-%m-%d")

    if "registration_date" in combined_df.columns:
        combined_df["registration_date"] = combined_df["registration_date"].apply(excel_serial_to_date)

    return combined_df

def save_supplier_json(dataframe, output_filename):
    os.makedirs("processed_data", exist_ok=True)
    output_path = os.path.join("processed_data", output_filename)
    dataframe.to_json(output_path, orient="records", force_ascii=False, indent=2)
    print(f"JSON saved to {output_path}")
