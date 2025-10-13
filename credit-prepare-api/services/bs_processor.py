import os
import pandas as pd
import re

RAW_DATA_FOLDER = "./raw_data/bs"
PROCESSED_DATA_FOLDER = "./processed_data"
OUTPUT_CSV_PATH = os.path.join(PROCESSED_DATA_FOLDER, "bs_all_processed_data.csv")

FINAL_HEADERS = [
    "Assets",
    "Cash and deposits at financial institutions",
    "Accounts receivable",
    "Accounts and notes receivable - net",
    "Total short-term loans consolidation",
    "Inventories-net",
    "Accrued income",
    "Prepaid expenses",
    "Other current assets",
    "Others - Total current assets",
    "Total current assets",
    "Total long-term loans and investments",
    "Property, plant and equipment - net",
    "Other non-current assets",
    "Others - Total non-current assets",
    "Total non-current assets",
    "Total assets",
    "Liabilities and shareholders' equity",
    "Liabilities",
    "Bank overdrafts and short-term loans from financial institutions",
    "Accounts payable",
    "Total accounts payable and notes payable",
    "Current portion of long-term loans",
    "Total short-term loans",
    "Accrued expenses",
    "Unearned revenues",
    "Other current liabilities",
    "Others - Total current liabilities",
    "Total current liabilities",
    "Total long-term loans",
    "Other non-current liabilities",
    "Others - Total non-current liabilities",
    "Total non-current liabilities",
    "Total Liabilities",
    "Shareholder's equity",
    "Authorized preferred stocks",
    "Authorized common stocks",
    "Issued and paid-up preferred stocks",
    "Issued and paid-up common stocks",
    "Appraisal surplus on property, plant and equipment",
    "Accumulated retained earnings",
    "Others",
    "Total shareholders' equity",
    "Total liabilities and shareholders' equity",
    "Additional information for shareholders' equity",
    "Common stocks",
    "No.of shares - Authorized",
    "Par value (Baht) - Authorized",
    "No.of shares - Issued and paid-up",
    "Par value (Baht) - Issued and Paid-up"
]

def process_bs_statements():
    os.makedirs(PROCESSED_DATA_FOLDER, exist_ok=True)
    processed_df = pd.DataFrame(columns=['company_id', 'company_name', 'year'] + FINAL_HEADERS)

    csv_files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith(".csv") and f.startswith("BS_")]

    for csv_file_name in csv_files:
        input_csv_path = os.path.join(RAW_DATA_FOLDER, csv_file_name)

        match = re.match(r"BS_(\d+)_([^_]+).*\.csv", csv_file_name)
        company_id = match.group(1) if match else ""
        company_name = match.group(2) if match else ""

        try:
            df = pd.read_csv(input_csv_path, encoding='latin1', header=None)
        except:
            df = pd.read_csv(input_csv_path, encoding='cp1252', header=None)

        years_from_header = []
        for col_index in range(3, df.shape[1]):
            year_str = df.iloc[0, col_index]
            year_match = re.search(r'\d{4}', str(year_str))
            if year_match:
                years_from_header.append(year_match.group(0))

        for i, current_year in enumerate(years_from_header):
            if not re.match(r'\d{4}', str(current_year)):
                continue

            row_data = {
                'company_id': company_id,
                'company_name': company_name,
                'year': current_year
            }

            values_for_year = df.iloc[3:53, 3 + i].tolist()

            for j, header in enumerate(FINAL_HEADERS):
                value = values_for_year[j] if j < len(values_for_year) else None
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.isna(numeric_value):
                        numeric_value = 0
                    if numeric_value == int(numeric_value):
                        numeric_value = int(numeric_value)
                except ValueError:
                    numeric_value = 0

                row_data[header] = numeric_value

            processed_df.loc[len(processed_df)] = row_data

    processed_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8')
    return {"message": "BS processed", "rows": len(processed_df), "data": processed_df}
