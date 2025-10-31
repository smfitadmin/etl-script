# dbd_web_scraping.py
# ============================================================
# DBD Scraper: Auto PDF (rename) + 3 XLS downloads + Company Title JSON
# - ดาวน์โหลด PDF ข้อมูลบริษัทจากปุ่ม id="printProfile"
# - Rename Report.pdf -> <juristic_id>_company_info.pdf
# - ดาวน์โหลดงบการเงิน 3 รายงาน (balance, income, ratios)
# - ดึงข้อมูลจากการ์ด "ข้อมูลนิติบุคคล" เป็น <juristic_id>_company_title.json
# ============================================================

import argparse
import time
import json
from pathlib import Path
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# Utilities
# ============================================================

def make_driver(download_dir: Path, headless: bool = False) -> webdriver.Chrome:
    opts = ChromeOptions()
    prefs = {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari")
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(download_dir)})
    return driver


def save_debug(driver, tag: str, out_dir: Path):
    try:
        ts = int(time.time())
        img = out_dir / f"debug_{tag}_{ts}.png"
        html = out_dir / f"debug_{tag}_{ts}.html"
        driver.save_screenshot(str(img))
        html.write_text(driver.page_source, encoding="utf-8")
        print(f"Screenshot saved: {img}")
        print(f"HTML saved: {html}")
    except Exception:
        pass


def try_close_popups(driver, loops=2):
    """ปิด popup หรือ dialog ที่ขวาง"""
    for _ in range(loops):
        for xp in [
            "//button[normalize-space()='ปิด']",
            "//button[contains(.,'ยอมรับ')]",
            "//button[contains(.,'ตกลง')]",
            "//div[contains(@class,'modal')]//button",
        ]:
            try:
                for el in driver.find_elements(By.XPATH, xp):
                    if el.is_displayed():
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.2)
            except Exception:
                pass


def wait_for_downloads(folder: Path, before_set: set, timeout=120) -> Path:
    """รอให้ไฟล์ใหม่ถูกดาวน์โหลด"""
    end = time.time() + timeout
    while time.time() < end:
        after = set(folder.glob("*"))
        new = [p for p in after - before_set if p.exists() and not p.name.endswith(".crdownload")]
        new = [p for p in new if not p.name.lower().endswith(".html")]
        if new:
            return sorted(new, key=lambda p: p.stat().st_mtime)[-1]
        time.sleep(0.5)
    raise TimeoutError("รอโหลดไฟล์ไม่ทันเวลา")


# ============================================================
# DBD Flow
# ============================================================

def search_by_juristic_id(driver, juristic_id: str):
    print(f"กำลังค้นหาเลขนิติบุคคล: {juristic_id}")
    driver.get("https://datawarehouse.dbd.go.th/index")
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)
    try_close_popups(driver)
    search_box = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, "//input[@type='text' and contains(@placeholder,'ค้นหา')]"))
    )
    search_box.clear()
    search_box.send_keys(juristic_id)
    time.sleep(0.3)
    search_box.send_keys(u"\ue007")  # Enter
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//*[contains(.,'ข้อมูลนิติบุคคล')]"))
    )
    print("พบหน้าข้อมูลนิติบุคคล")


# ==== NEW: scrape "ข้อมูลนิติบุคคล" card and write <juristic_id>_company_title.json
def scrape_company_title_card(driver, out_dir: Path, juristic_id: str) -> Path:
    """
    หา card ที่มีหัวข้อ 'ข้อมูลนิติบุคคล' แล้วดึงคู่ label/value ภายใน .row
    คืน path ของไฟล์ JSON ที่บันทึก
    """
    try_close_popups(driver)
    # เลื่อนหน้าจอให้เห็นการ์ด
    try:
        el_title = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//h5[contains(@class,'card-title')][contains(.,'ข้อมูลนิติบุคคล')]"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el_title)
        time.sleep(0.3)
    except Exception:
        save_debug(driver, "company_title_not_found", out_dir)
        raise RuntimeError("ไม่พบการ์ด 'ข้อมูลนิติบุคคล'")

    card = driver.find_element(By.XPATH, "//h5[contains(@class,'card-title')][contains(.,'ข้อมูลนิติบุคคล')]/ancestor::div[contains(@class,'card-infos')]")
    rows = card.find_elements(By.CSS_SELECTOR, ".card-body .row .col-6")

    def norm_txt(s: str) -> str:
        return " ".join((s or "").replace("\xa0", " ").split()).strip()

    # ==== NEW: helper แปลงวันที่ไทย (เช่น 27 ก.ค. 2537 → 1994-07-27)
    MONTHS_TH = {
        "ม.ค.": 1, "ก.พ.": 2, "มี.ค.": 3, "เม.ย.": 4, "พ.ค.": 5, "มิ.ย.": 6,
        "ก.ค.": 7, "ส.ค.": 8, "ก.ย.": 9, "ต.ค.": 10, "พ.ย.": 11, "ธ.ค.": 12
    }
    def thai_date_to_iso(date_text: str) -> str | None:
        try:
            parts = date_text.strip().split()
            if len(parts) != 3:
                return None
            day = int(parts[0])
            month_th = parts[1]
            year_th = int(parts[2])
            month = MONTHS_TH.get(month_th)
            if not month:
                return None
            year = year_th - 543 if year_th > 2400 else year_th
            return f"{year:04d}-{month:02d}-{day:02d}"
        except Exception:
            return None

    data = {
        "entity_type": None,
        "entity_status": None,
        "incorporation_date_th_text": None,
        "registered_date": None,  # ✅ เพิ่ม key สำหรับ YYYY-MM-DD
        "registered_capital_text": None,
        "old_registration_no": None,
        "business_group": None,
        "business_size": None,
        "financial_filing_years_th": [],
        "head_office_address": None,
        "website": None,
    }

    i = 0
    while i < len(rows) - 1:
        label = norm_txt(rows[i].text)
        value_el = rows[i + 1]

        if "ปีที่ส่งงบการเงิน" in label:
            yrs = []
            spans = value_el.find_elements(By.CSS_SELECTOR, ".tab1fiscal")
            if spans:
                for sp in spans:
                    yrs.append((sp.get_attribute("title") or norm_txt(sp.text)))
            else:
                yrs = [y for y in norm_txt(value_el.text).split() if y.isdigit()]
            data["financial_filing_years_th"] = [y for y in yrs if y]
            i += 2
            continue

        val = norm_txt(value_el.text)

        if label == "ประเภทนิติบุคคล":
            data["entity_type"] = val or None
        elif label == "สถานะนิติบุคคล":
            data["entity_status"] = val or None
        elif label == "วันที่จดทะเบียนจัดตั้ง":
            data["incorporation_date_th_text"] = val or None
            data["registered_date"] = thai_date_to_iso(val)  # ✅ แปลงเพิ่มตรงนี้
        elif label == "ทุนจดทะเบียน":
            data["registered_capital_text"] = val or None
        elif label == "เลขทะเบียนเดิม":
            data["old_registration_no"] = val or None
        elif label == "กลุ่มธุรกิจ":
            data["business_group"] = val or None
        elif label == "ขนาดธุรกิจ":
            data["business_size"] = val or None
        elif label == "ที่ตั้งสำนักงานแห่งใหญ่":
            data["head_office_address"] = val or None
        elif label == "Website":
            data["website"] = (val if val and val != "-" else None)
        i += 2

    out_path = out_dir / f"{juristic_id}_company_title.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"บันทึก Company Title JSON: {out_path.name}")
    return out_path



def download_company_info_pdf(driver, juristic_id: str, out_dir: Path) -> Path:
    print("กำลังดาวน์โหลด PDF ข้อมูลนิติบุคคล...")
    try_close_popups(driver, loops=2)

    before = set(out_dir.glob("*"))
    try:
        btn = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, "printProfile")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.3)
        btn.click()
    except Exception:
        save_debug(driver, "printProfile_not_found", out_dir)
        raise RuntimeError("ไม่พบปุ่มพิมพ์ข้อมูล (id=printProfile)")

    print("รอดาวน์โหลดไฟล์ PDF...")
    try:
        pdf_file = wait_for_downloads(out_dir, before, timeout=90)
    except TimeoutError:
        print("ไม่พบไฟล์ PDF ที่ดาวน์โหลด")
        save_debug(driver, "pdf_timeout", out_dir)
        raise

    # rename Report.pdf -> <juristic_id>_company_info.pdf
    if pdf_file and pdf_file.suffix.lower() == ".pdf":
        new_path = out_dir / f"{juristic_id}_company_info.pdf"
        if new_path.exists():
            new_path.unlink()
        pdf_file.rename(new_path)
        pdf_file = new_path
        print(f"เปลี่ยนชื่อไฟล์ PDF เป็น: {pdf_file.name}")
    else:
        raise RuntimeError("ไม่พบไฟล์ PDF ที่ถูกต้อง (อาจได้ .html)")

    print(f"ดาวน์โหลด PDF สำเร็จ: {pdf_file.name}")
    return pdf_file


def go_financial_tab(driver):
    print("กำลังเปิดแท็บข้อมูลงบการเงิน...")
    try_close_popups(driver)
    time.sleep(1.0)

    # scroll เพื่อให้แท็บโผล่
    driver.execute_script("window.scrollTo(0, 600);")
    time.sleep(1.0)

    patterns = [
        "//a[contains(.,'งบการเงิน') and not(contains(@href,'#'))]",
        "//a[contains(@href,'#tab22') or contains(@href,'#tab_financial')]",
        "//button[contains(.,'งบการเงิน')]",
        "//li[contains(@class,'dropdown')]//*[contains(.,'งบการเงิน')]",
        "//*[contains(text(),'งบการเงิน') and (self::a or self::span or self::div)]"
    ]

    found = False
    for xp in patterns:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if el.is_displayed():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.5)
                    try:
                        el.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", el)
                    time.sleep(2.5)
                    found = True
                    break
            if found:
                break
        except Exception:
            continue

    if not found:
        save_debug(driver, "financial_tab_not_found", Path("./downloads"))
        raise RuntimeError("ไม่พบแท็บ 'งบการเงิน'")

    # รอปุ่ม finMenu ปรากฏ
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".finMenu"))
        )
        time.sleep(2.5)
        print("เนื้อหางบการเงินโหลดสำเร็จ")
    except Exception:
        save_debug(driver, "financial_content_not_loaded", Path("./downloads"))
        raise RuntimeError("แท็บงบการเงินเปิดแล้ว แต่เนื้อหาไม่โหลด")

def switch_report(driver, lang_key: str):
    btn = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, f".finMenu[lang='{lang_key}']"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    btn.click()
    WebDriverWait(driver, 10).until(
        lambda d: "active" in d.find_element(By.CSS_SELECTOR, f".finMenu[lang='{lang_key}']").get_attribute("class")
    )


def click_excel(driver, out_dir: Path) -> Path:
    toggle = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'dropdown') and contains(@class,'print')]//a"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", toggle)
    toggle.click()
    menu = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, "//ul[contains(@class,'dropdown-menu') and (contains(@class,'show') or contains(@style,'display: block'))]"))
    )
    link = menu.find_element(By.XPATH, ".//a[@id='finXLS']")
    before = set(out_dir.glob("*"))
    link.click()
    file = wait_for_downloads(out_dir, before, timeout=180)
    return file


def download_reports(driver, out_dir: Path, juristic_id: str):
    reports = {
        "balancesheet": "balance",
        "profitloss": "income",
        "ratio": "ratios",
    }
    for lang, suffix in reports.items():
        print(f"ดาวน์โหลด {suffix} ...")
        switch_report(driver, lang)
        f = click_excel(driver, out_dir)
        newp = out_dir / f"{juristic_id}_{suffix}.xls"
        if newp.exists():
            newp.unlink()
        f.rename(newp)
        print(f"ดาวน์โหลดสำเร็จ: {newp.name}")


# ============================================================
# Main
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--juristic-id", required=True)
    ap.add_argument("--out-dir", default="./downloads")
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    print("=" * 60)
    print("DBD Financial Scraper (Auto PDF + 3 XLS + Company Title JSON)")
    print("=" * 60)

    driver = make_driver(out_dir, headless=args.headless)
    try:
        search_by_juristic_id(driver, args.juristic_id)

        # ==== NEW: ดึงการ์ด "ข้อมูลนิติบุคคล" ก่อน เพื่อบันทึก company_title.json
        try:
            scrape_company_title_card(driver, out_dir, args.juristic_id)
        except Exception as e:
            print(f"[warn] company_title.json: {e}")

        download_company_info_pdf(driver, args.juristic_id, out_dir)
        go_financial_tab(driver)
        download_reports(driver, out_dir, args.juristic_id)

        print("=" * 60)
        print("เสร็จสมบูรณ์:")
        print(f"- {args.juristic_id}_company_title.json")   # ==== NEW: line
        print(f"- {args.juristic_id}_company_info.pdf")
        print(f"- {args.juristic_id}_balance.xls")
        print(f"- {args.juristic_id}_income.xls")
        print(f"- {args.juristic_id}_ratios.xls")
        print("=" * 60)

    except Exception as e:
        print(f"\nเกิดข้อผิดพลาด: {e}")
        save_debug(driver, "final_error", out_dir)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()