# 📘 ขั้นตอนการดึงและนำเข้าข้อมูลจาก DBD Data Warehouse

## 🔹 ตัวอย่างเลขนิติบุคคล
```
0105537086874
```

---

## ⚙️ ขั้นตอนที่ 1 — Web Scraping ข้อมูลบริษัทจาก DBD

ดาวน์โหลดไฟล์ทั้งหมดของบริษัทจากเว็บไซต์ DBD:

```bash
python dbd_web_scraping.py --juristic-id 0105537086874 --out-dir ./downloads
```

> ✅ ผลลัพธ์ในโฟลเดอร์ `downloads/`
> - `0105537086874_company_info.pdf`
> - `0105537086874_balance.xls`
> - `0105537086874_income.xls`
> - `0105537086874_ratios.xls`

---

## 🧾 ขั้นตอนที่ 2 — OCR ข้อมูลจาก PDF

แปลงข้อมูลบริษัทจาก PDF (`_company_info.pdf`) เป็น JSON ที่อ่านได้:

```bash
python pdf_ocr_dbd_to_json.py downloads/0105537086874_company_info.pdf
```

> ✅ ผลลัพธ์ในโฟลเดอร์ `downloads/`
> - `0105537086874_company_info.json`
> - `0105537086874_company_info_structured.json`

---

## 📊 ขั้นตอนที่ 3 — แปลงไฟล์งบดุล (Balance Sheet)

อ่านไฟล์ Excel แล้วแปลงเป็น JSON:

```bash
python script_read_dbd_balance.py   --folder ./downloads   --outdir ./processed_data   --debug
```

> ✅ ผลลัพธ์:
> - `processed_data/0105537086874_balance.json`

---

## 💰 ขั้นตอนที่ 4 — แปลงไฟล์งบกำไรขาดทุน (Income Statement)

```bash
python script_read_dbd_income.py   --folder ./downloads   --outdir ./processed_data   --debug
```

> ✅ ผลลัพธ์:
> - `processed_data/0105537086874_income.json`

---

## 📈 ขั้นตอนที่ 5 — แปลงไฟล์อัตราส่วนทางการเงิน (Financial Ratios)

```bash
python script_read_dbd_ratios.py   --folder ./downloads   --outdir ./processed_data   --debug
```

> ✅ ผลลัพธ์:
> - `processed_data/0105537086874_ratios.json`

---

## 🧩 ขั้นตอนที่ 6 — ส่งข้อมูลบริษัทเข้าสู่ระบบ API

คัดลอกเนื้อหาในไฟล์  
`downloads/0105537086874_company_info_structured.json`  
แล้ว POST ไปยัง API endpoint:

```
POST <URL>/api/public/dbd-company-supplier
```

ตัวอย่างการเรียกใช้งานด้วย `curl`:

```bash
curl -X POST <URL>/api/public/dbd-company-supplier   -H "Content-Type: application/json"   -d @downloads/0105537086874_company_info_structured.json
```

---

## 🗂️ ขั้นตอนที่ 7 — ย้ายไฟล์ JSON ไปไว้ในระบบ Laravel

คัดลอกไฟล์ JSON ทั้งหมดจาก `processed_data/`  
ไปไว้ในโฟลเดอร์:

```
smf-api/storage/app/
```

> ✅ ตัวอย่างไฟล์:
> - `0105537086874_balance.json`
> - `0105537086874_income.json`
> - `0105537086874_ratios.json`

---

## 🧠 ขั้นตอนที่ 8 — Import ข้อมูลเข้าฐานข้อมูล MySQL

ใช้คำสั่ง Artisan เพื่อ import ข้อมูลทั้งหมดเข้าสู่ฐานข้อมูล:

### 🔸 Import เฉพาะบริษัท (ระบุเลขนิติบุคคล)
```bash
php artisan dbd:import-financial --tax_id=0105537086874
```

> ✅ ระบบจะอ่านเฉพาะไฟล์ที่มีเลขนิติบุคคลที่กำหนด  
> เช่น `0105537086874_balance.json`, `0105537086874_income.json`, `0105537086874_ratios.json`  
> และบันทึกข้อมูลลงใน MySQL โดยอัตโนมัติ:
> - `company_balance_sheet`
> - `company_income_statement`
> - `company_financial_ratios`

---

### 🔸 Import ทุกบริษัท (ทุกไฟล์ใน storage/app)

หากไม่ระบุ `--tax_id` ระบบจะ import ข้อมูลจาก **ทุกไฟล์ JSON**  
ที่มีชื่อรูปแบบ `_balance.json`, `_income.json`, `_ratios.json` ภายใต้ `storage/app/`  
โดยอัตโนมัติ เช่น

```bash
php artisan dbd:import-financial
```

> ✅ ตัวอย่างไฟล์ที่จะถูกอ่าน:
> - `storage/app/0105537086874_balance.json`
> - `storage/app/0105537086874_income.json`
> - `storage/app/0105537086874_ratios.json`
> - `storage/app/0105556009871_balance.json`
> - `storage/app/0105556009871_income.json`
> - ...

ระบบจะวนลูปนำเข้าทีละบริษัทโดยตรวจสอบเลขนิติบุคคลจากชื่อไฟล์  
และบันทึกลงในฐานข้อมูลตามประเภท (Balance / Income / Ratios)

---

## 🧑‍💼 ขั้นตอนที่ 9 — เรียกดูข้อมูลกรรมการบริษัท (Directors API)

คุณสามารถดึงข้อมูลกรรมการบริษัทได้โดยตรงจาก API:

```
GET /api/public/directors/{tax_id}
```

ตัวอย่างการใช้งาน:

```bash
curl -X GET <URL>/api/public/directors/0105541008416
```

> ✅ ตัวอย่างผลลัพธ์:
> ```json
> {
>   "total": 3,
>   "data": [
>     { "prefix": "นาย", "first_name": "อิทธิพัฒน์", "last_name": "จงวัฒนาภิรมย์" },
>     { "prefix": "นาง", "first_name": "ศรีเพ็ญ", "last_name": "จงวัฒนาภิรมย์" },
>     { "prefix": "นางสาว", "first_name": "ศิริวรรณ", "last_name": "จงวัฒนาภิรมย์" }
>   ]
> }
> ```

---

## 🎯 สรุปลำดับการทำงานทั้งหมด

| ลำดับ | ขั้นตอน | คำสั่งหลัก |
|:--:|:--------------------------|:--------------------------------------------|
| 1 | ดึงข้อมูลจาก DBD | `python dbd_web_scraping.py --juristic-id ...` |
| 2 | OCR ข้อมูลบริษัท | `python pdf_ocr_dbd_to_json.py` |
| 3 | แปลงงบดุล | `python script_read_dbd_balance.py` |
| 4 | แปลงงบกำไรขาดทุน | `python script_read_dbd_income.py` |
| 5 | แปลงอัตราส่วนทางการเงิน | `python script_read_dbd_ratios.py` |
| 6 | ส่งข้อมูลบริษัทเข้า API | `POST /api/public/dbd-company-supplier` |
| 7 | ย้ายไฟล์ JSON ไป storage | `storage/app/<tax_id>_*.json` |
| 8 | นำข้อมูลเข้า MySQL | `php artisan dbd:import-financial [--tax_id=<เลขนิติบุคคล>]` |
| 9 | ดึงข้อมูลกรรมการบริษัท | `GET /api/public/directors/{tax_id}` |

---

## 🏁 ผลลัพธ์สุดท้าย

ข้อมูลของบริษัทจะถูกบันทึกไว้ในระบบ MySQL  
และสามารถเรียกดูได้ผ่าน API:

- `/api/company/{tax_id}/financial/{year}` → ข้อมูลรายปี  
- `/api/company/{tax_id}/financial` → รวมข้อมูลทุกปี  
- `/api/public/directors/{tax_id}` → ข้อมูลกรรมการบริษัท
