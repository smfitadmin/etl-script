# README — คู่มือใช้งานสคริปต์แปลงไฟล์ + Laravel Docker Setup

## 1) สิ่งที่ต้องมีล่วงหน้า (Prerequisites)

- Python 3.9+
- pip และ (แนะนำ) virtualenv/conda
- สำหรับสคริปต์ PDF:
  - Java
  - poppler
  - Tesseract OCR
- Laravel + Docker + Composer

ติดตั้ง Python dependencies:
```bash
pip install camelot-py tabula-py pdf2image pytesseract pandas tqdm pillow
```

---

## 2) โครงสร้างโปรเจกต์

```
project-root/
├─ raw_data/
│  ├─ inv/
│  ├─ po/
│  └─ rm/
├─ processed_data/
│  ├─ inv/
│  ├─ po/
│  └─ rm/
├─ storage/app/
├─ be/                # Laravel backend
│  ├─ Dockerfile
│  └─ composer.json
├─ docker-compose.yml
└─ ...
```

---

## 3) แปลงไฟล์ PDF/CSV → JSON

### PDF (Invoice)
```bash
python pdf_ocr_to_json.py invoice_detail_report_20251003_72195.pdf --method table --engine tabula --records-only
python pdf_ocr_to_json.py invoice_detail_report_20251003_72195.pdf --method table --engine tabula --records-only --fix-lookalikes
```

### Remittance Advice
```bash
python pdf_ocr_rm_to_json.py cpall_remittance_advice_report_20251007_72195.pdf
```

### PO CSV
```bash
python read_po_csv_to_json.py --csv "raw_data/po/po_detail_report_20251007_2050363.csv"
```

---

## 4) Laravel Artisan Commands

### Invoices
```bash
php artisan invoices:import storage/app/inv_2023.json --table=gec_inv_2023
php artisan invoices:import storage/app/inv_2025.json --table=gec_inv_2025
```

### PO
```bash
php artisan po:import storage/app/po_2025.json --table=gec_po_2025
```

### Reports
```bash
php artisan rm-report:import storage/app/cpall_remittance_advice_report_20251007_2050363.json --table=rm_detail_report
php artisan po-report:import storage/app/po_detail_report_20251007_2050363.json --table=po_detail_report
php artisan invoice-report:import storage/app/invoice_detail_report_20251003_2050363.json --table=invoice_detail_report
```

---

## 5) รัน Laravel ด้วย Docker

### docker-compose.yml
```yaml
version: "3.8"
services:
  laravel:
    build:
      context: ./be
      dockerfile: Dockerfile
    container_name: laravel-app
    restart: unless-stopped
    working_dir: /var/www
    volumes:
      - ./be:/var/www
      - ./storage:/var/www/storage
    ports:
      - "8000:8000"
    depends_on:
      - mysql
    networks:
      - laravel-net

  mysql:
    image: mysql:8.0
    container_name: mysql-db
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: smf
      MYSQL_USER: smf_user
      MYSQL_PASSWORD: smf_pass
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    networks:
      - laravel-net

  phpmyadmin:
    image: phpmyadmin/phpmyadmin:latest
    container_name: phpmyadmin
    restart: unless-stopped
    ports:
      - "8080:80"
    environment:
      PMA_HOST: mysql
      MYSQL_ROOT_PASSWORD: root
    depends_on:
      - mysql
    networks:
      - laravel-net

networks:
  laravel-net:

volumes:
  mysql_data:
```

### Dockerfile (ใน be/)
```dockerfile
FROM php:8.2-fpm
RUN apt-get update && apt-get install -y git zip unzip curl libpng-dev libonig-dev libxml2-dev libzip-dev \ 
    && docker-php-ext-install pdo_mysql mbstring exif pcntl bcmath gd zip
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer
WORKDIR /var/www
COPY . .
RUN composer install --no-interaction --optimize-autoloader --no-dev
EXPOSE 8000
CMD php artisan serve --host=0.0.0.0 --port=8000
```

### Run Project
```bash
docker-compose build
docker-compose up -d
docker exec -it laravel-app bash
composer install
cp .env.example .env
php artisan key:generate
php artisan migrate
```

เข้าผ่านเว็บ: [http://localhost:8000](http://localhost:8000)
phpMyAdmin: [http://localhost:8080](http://localhost:8080)

---

## 6) Troubleshooting

| ปัญหา | วิธีแก้ |
|--------|----------|
| Connection refused | ตรวจว่า DB_HOST=mysql |
| Composer SSL error | เพิ่ม composer config repo.packagist https://packagist.org |
| Laravel restart | ตรวจสิทธิ์โฟลเดอร์ `vendor/` |
| Port 8000 ถูกใช้ | เปลี่ยนเป็น 8010:8000 |
