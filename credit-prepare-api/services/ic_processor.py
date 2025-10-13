import os
import pandas as pd
import re

RAW_DATA_FOLDER = "./raw_data/ic"
PROCESSED_DATA_FOLDER = "./processed_data"
OUTPUT_CSV_PATH = os.path.join(PROCESSED_DATA_FOLDER, "ic_all_processed_data.csv")

IC_HEADERS = [
    "Revenues from sales and services",
    "Cost of sales and services",
    "Gross profit (loss)",
    "Other incomes",
    "Profit (loss) before expenses",
    "Selling expenses",
    "Administrative expenses",
    "Other expenses",
    "Profit (loss) before finance costs and income tax",
    "Finance costs",
    "Profit (loss) before income tax",
    "Income tax expense",
    "Net profit (loss)",
    "Basic earnings (loss) per share",
]

def process_ic_statements():
    os.makedirs(PROCESSED_DATA_FOLDER, exist_ok=True)
    processed_df = pd.DataFrame(columns=["company_id", "company_name", "year"] + IC_HEADERS)

    files = [f for f in os.listdir(RAW_DATA_FOLDER) if f.endswith(".csv")]

    for file in files:
        if not file.startswith("IC_"):
            continue

        match = re.match(r"IC_(\d+)_([^_]+).*\.csv", file)
        company_id = match.group(1) if match else ""
        company_name = match.group(2) if match else ""

        input_csv_path = os.path.join(RAW_DATA_FOLDER, file)

        try:
            df = pd.read_csv(input_csv_path, encoding='latin1', header=None)
        except UnicodeDecodeError:
            df = pd.read_csv(input_csv_path, encoding='cp1252', header=None)

        years_from_header = []
        for col_index in range(3, df.shape[1]):
            year_str = df.iloc[0, col_index]
            year_match = re.search(r'\d{4}', str(year_str))
            if year_match:
                years_from_header.append(year_match.group(0))

        for i, current_year in enumerate(years_from_header):
            row_data = {
                "company_id": company_id,
                "company_name": company_name,
                "year": current_year
            }

            values = df.iloc[3:3 + len(IC_HEADERS), 3 + i].tolist()

            for j, header in enumerate(IC_HEADERS):
                value = values[j] if j < len(values) else None
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
    return {"message": "IC processed", "rows": len(processed_df), "data": processed_df }
