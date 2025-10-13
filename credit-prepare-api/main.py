from fastapi import FastAPI
from services.bs_processor import process_bs_statements
from services.ic_processor import process_ic_statements
# from services.po_processor import load_po_data, save_po_json
# from services.inv_processor import load_invoice_data, save_inv_json
from services.supplier_processor import load_supplier_data, save_supplier_json

from services.inv_old_processor import load_old_invoice_data, save_old_inv_json
from services.po_old_processor import load_old_po_data, save_old_po_json

# from services.scraper import scrape_company_by_id
import requests
import os
from dotenv import load_dotenv

# โหลดค่า environment จาก .env
load_dotenv()
API_BASE = os.getenv("API_BASE", "")
API_HEADERS = {"Content-Type": "application/json"}

app = FastAPI(
    title="Credit Scoring Preparing API",
    description="API สำหรับประมวลผลข้อมูลงบการเงิน (BS/IC) และส่งไป API ปลายทาง",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "Welcome to Credit Scoring Preparing API"}

@app.post("/process-bs")
def process_bs():
    result = process_bs_statements()
    send_to_api = True

    if send_to_api and "data" in result:
        try:
            processed_df = result["data"]
            json_data = processed_df.to_json(orient='records')
            endpoint = f"{API_BASE}/api/public/bol-bs"

            response = requests.post(endpoint, headers=API_HEADERS, data=json_data)
            response.raise_for_status()

            return {
                "message": "BS processed and sent",
                "status_code": response.status_code,
                "rows": len(processed_df)
            }
        except Exception as e:
            return {"message": "BS processed but failed to send", "error": str(e)}
    return result


@app.post("/process-ic")
def process_ic():
    result = process_ic_statements()
    send_to_api = True

    if send_to_api and "data" in result:
        try:
            processed_df = result["data"]
            json_data = processed_df.to_json(orient='records')
            endpoint = f"{API_BASE}/api/public/bol-ic"

            response = requests.post(endpoint, headers=API_HEADERS, data=json_data)
            response.raise_for_status()

            return {
                "message": "IC processed and sent",
                "status_code": response.status_code,
                "rows": len(processed_df)
            }
        except Exception as e:
            return {"message": "IC processed but failed to send", "error": str(e)}
    return result


from datetime import datetime, timedelta

def excel_serial_to_thai_date(serial: int) -> str:
    base_date = datetime(1899, 12, 30)  # Excel เริ่มจากวันที่นี้
    date = base_date + timedelta(days=serial)
    day = date.day
    return f"{day:02d}/{date.month}/{(date.year)-543}"



def main():
    # po_filename = "Transaction Data Potential customer list - 7.xlsx"
    # inv_filename = "Inv Transaction Data Potential customer list - 7.xlsx"
    # supplier_filename = "Supplier_Data_MappingDBD.xlsx"

    # po_data = load_po_data(po_filename)
    # po_output_json = "po_data.json"
    # save_po_json(po_data, po_output_json)

    # inv_data = load_invoice_data(inv_filename)
    # inv_output_json = "inv_data.json"
    # save_inv_json(inv_data, inv_output_json)

    # supplier_data = load_supplier_data(supplier_filename)
    # supplier_output_json = "supplier_data.json"
    # save_supplier_json(supplier_data, supplier_output_json)

    inv_filenames = [
        {
            "input_filename":"inv/Invoice_052025_new.csv", 
            "output_filename":"invoice_052025_new.json"
         },
        {
            "input_filename":"inv/Invoice_062025_new.csv", 
            "output_filename":"invoice_062025_new.json"
         },
        #  {
        #     "input_filename":"inv/invoice_042025.csv", 
        #     "output_filename":"invoice_042025.json"
        #  },
        #  {
        #     "input_filename":"inv/invoice_052025.csv", 
        #     "output_filename":"invoice_052025.json"
        #  },
        #  {
        #     "input_filename":"inv/invoice_062025.csv", 
        #     "output_filename":"invoice_062025.json"
        #  },
        #  {
        #     "input_filename":"inv/invoice_072025.csv", 
        #     "output_filename":"invoice_072025.json"
        #  },
        #  {
        #     "input_filename":"inv/invoice_082025.csv", 
        #     "output_filename":"invoice_082025.json"
        #  },
    ]
    
    if (len(inv_filenames) > 0) :
        for inv_filename in inv_filenames:
            print(inv_filename["input_filename"])
            inv_data = load_old_invoice_data(inv_filename["input_filename"])
            print(len(inv_data))
            print(inv_filename["output_filename"])
            save_old_inv_json(inv_data, inv_filename["output_filename"])

    po_filenames = [
        # {
        #     "input_filename":"po/PO_022025.csv", 
        #     "output_filename":"po_022025.json"
        #  },
        # {
        #     "input_filename":"po/PO_032025.csv", 
        #     "output_filename":"po_032025.json"
        #  },
        # {
        #     "input_filename":"po/PO_042025.csv", 
        #     "output_filename":"po_042025.json"
        #  },
        # {
        #     "input_filename":"po/PO_052025.csv", 
        #     "output_filename":"po_052025.json"
        #  },
        # {
        #     "input_filename":"po/PO_062025.csv", 
        #     "output_filename":"po_062025.json"
        #  },
        # {
        #     "input_filename":"po/PO_072025.csv", 
        #     "output_filename":"po_072025.json"
        #  },
        #  {
        #     "input_filename":"po/PO_082025.csv", 
        #     "output_filename":"po_082025.json"
        #  },
    ]

    if (len(po_filenames) > 0) :
        for po_filename in po_filenames:
            po_data = load_old_po_data(po_filename["input_filename"])
            print(len(po_data))
            print(po_filename["output_filename"])
            save_old_po_json(po_data, po_filename["output_filename"])
    

if __name__ == "__main__":
    main()
