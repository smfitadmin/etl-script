# dbd_web_scraping.py
# ============================================================
# DBD Scraper: Auto PDF (rename) + 3 XLS downloads + Company Title JSON
# - ดาวน์โหลด PDF ข้อมูลบริษัทจากปุ่ม id="printProfile"
# - Rename Report.pdf -> <juristic_id>_company_info.pdf
# - ดาวน์โหลดงบการเงิน 3 รายงาน (balance, income, ratios)
# - ดึงข้อมูลจากการ์ด "ข้อมูลนิติบุคคล" เป็น <juristic_id>_company_title.json
# - รองรับหลายรหัส โดยใช้ช่องค้นหาเดิม (#textSearch/#searchicon) ไม่โหลดหน้าใหม่
# - เมื่อเข้าแท็บ "ข้อมูลงบการเงิน" แล้วพบ <h3>ไม่พบข้อมูล</h3> ให้บันทึก JSON และข้ามการดาวน์โหลด
# ============================================================

import argparse
import time
import json
from pathlib import Path
from typing import List, Optional

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

    # คง session/cookies เดิมเพื่อความเสถียร
    profile_dir = str((Path("./chrome_profile")).resolve())
    opts.add_argument(f"--user-data-dir={profile_dir}")

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

    # ปรับให้ดูเหมือนผู้ใช้จริง
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")
    opts.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_argument("accept-language=th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7")

    if headless:
        # หากถูกบล็อกง่าย ให้พิจารณาไม่ใช้ headless
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    # ตั้งค่าเส้นทางดาวน์โหลดสำหรับบางเวอร์ชัน
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(download_dir)})
    except Exception:
        pass
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
    """โหลดหน้า index หนึ่งครั้ง แล้วค้นหาบริษัทแรกด้วยวิธีเดิม"""
    print(f"กำลังค้นหาเลขนิติบุคคล: {juristic_id}")
    driver.get("https://datawarehouse.dbd.go.th/index")
    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(1)
    try_close_popups(driver)

    search_box = WebDriverWait(driver, 15).until(
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


def search_via_header_input(driver, juristic_id: str, out_dir: Path):
    """
    ใช้ช่อง input #textSearch + #searchicon บนหน้าเดิมเพื่อเปลี่ยนบริษัท
    โดยไม่ต้อง driver.get(...) ใหม่
    """
    try_close_popups(driver)

    # บางครั้ง input อยู่บนสุดของหน้า
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.3)

    inp = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "input#textSearch"))
    )

    # เคลียร์ค่าเดิม + ใส่ค่าใหม่ผ่าน JS เพื่อเลี่ยงปัญหา send_keys
    driver.execute_script("""
      const el = arguments[0], val = arguments[1];
      el.focus();
      el.value = '';
      el.dispatchEvent(new Event('input', {bubbles:true}));
      el.value = val;
      el.dispatchEvent(new Event('input', {bubbles:true}));
    """, inp, juristic_id)

    # คลิกไอคอนค้นหา
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "#searchicon")
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        inp.send_keys(u"\ue007")

    # รอให้เนื้อหาใหม่โหลด (ดูจากข้อความและมีรหัสที่ขอใน source)
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, "//*[contains(.,'ข้อมูลนิติบุคคล')]"))
    )
    WebDriverWait(driver, 30).until(lambda d: juristic_id in d.page_source)

    try_close_popups(driver)
    time.sleep(0.5)
    print(f"เปลี่ยนบริษัทสำเร็จ -> {juristic_id}")


def scrape_company_title_card(driver, out_dir: Path, juristic_id: str) -> Path:
    """
    หา card 'ข้อมูลนิติบุคคล' แล้วดึงคู่ label/value ภายใน .row
    คืน path ของไฟล์ JSON ที่บันทึก
    """
    try_close_popups(driver)
    # เลื่อนให้เห็นการ์ด
    try:
        el_title = WebDriverWait(driver, 15).until(
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

    MONTHS_TH = {
        "ม.ค.": 1, "ก.พ.": 2, "มี.ค.": 3, "เม.ย.": 4, "พ.ค.": 5, "มิ.ย.": 6,
        "ก.ค.": 7, "ส.ค.": 8, "ก.ย.": 9, "ต.ค.": 10, "พ.ย.": 11, "ธ.ค.": 12
    }

    def thai_date_to_iso(date_text: str) -> Optional[str]:
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
        "registered_date": None,  # YYYY-MM-DD
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
            data["registered_date"] = thai_date_to_iso(val)
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
        btn = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "printProfile")))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.3)
        btn.click()
    except Exception:
        save_debug(driver, "printProfile_not_found", out_dir)
        raise RuntimeError("ไม่พบปุ่มพิมพ์ข้อมูล (id=printProfile)")

    print("รอดาวน์โหลดไฟล์ PDF...")
    try:
        pdf_file = wait_for_downloads(out_dir, before, timeout=120)
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


def go_financial_tab(driver, out_dir: Path) -> str:
    """
    เปิดแท็บ 'ข้อมูลงบการเงิน' แล้วรอหนึ่งในสองสภาวะ:
      1) พบเมนูรายงาน (.finMenu) -> คืนค่า 'menu'
      2) พบข้อความ 'ไม่พบข้อมูล' ใน card-infos -> คืนค่า 'empty'
    ถ้าไม่เจอทั้งคู่ภายในเวลา -> error
    """
    print("กำลังเปิดแท็บข้อมูลงบการเงิน...")
    try_close_popups(driver)
    time.sleep(0.8)

    # scroll ให้แท็บโผล่
    driver.execute_script("window.scrollTo(0, 600);")
    time.sleep(0.8)

    # ลองคลิกเข้า "งบการเงิน"
    patterns = [
        "//a[contains(@href,'#tab22') or contains(@href,'#tab_financial')]",
        "//a[contains(.,'งบการเงิน') and not(contains(@href,'#'))]",
        "//button[contains(.,'งบการเงิน')]",
        "//li[contains(@class,'dropdown')]//*[contains(.,'งบการเงิน')]",
        "//*[contains(text(),'งบการเงิน') and (self::a or self::span or self::div)]"
    ]
    for xp in patterns:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if el.is_displayed():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.3)
                    try:
                        el.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", el)
                    time.sleep(1.2)
                    break
        except Exception:
            continue

    # รอเงื่อนไขอย่างใดอย่างหนึ่งเกิดขึ้น
    deadline = time.time() + 20  # วินาที
    found_menu = False
    found_empty = False

    while time.time() < deadline:
        try_close_popups(driver, loops=1)

        # เงื่อนไข 1: มีเมนูรายงาน (.finMenu)
        try:
            menus = driver.find_elements(By.CSS_SELECTOR, ".finMenu")
            if any(m.is_displayed() for m in menus):
                found_menu = True
        except Exception:
            pass

        # เงื่อนไข 2: มีแถบข้อความไม่พบข้อมูล ใน card-infos ของงบการเงิน
        try:
            empties = driver.find_elements(
                By.XPATH,
                "//div[contains(@class,'card-infos')]//h3[normalize-space()='ไม่พบข้อมูล']"
            )
            if any(e.is_displayed() for e in empties):
                found_empty = True
        except Exception:
            pass

        if found_menu or found_empty:
            break
        time.sleep(0.5)

    if found_menu:
        print("เนื้อหางบการเงินโหลดสำเร็จ (มี .finMenu)")
        return "menu"

    if found_empty:
        print("งบการเงิน: ไม่พบข้อมูล (พบ <h3>ไม่พบข้อมูล</h3>)")
        return "empty"

    save_debug(driver, "financial_content_timeout", out_dir)
    raise RuntimeError("แท็บงบการเงินเปิดแล้ว แต่ไม่พบทั้งเมนูและ 'ไม่พบข้อมูล'")


def switch_report(driver, lang_key: str):
    btn = WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, f".finMenu[lang='{lang_key}']"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    btn.click()
    WebDriverWait(driver, 10).until(
        lambda d: "active" in d.find_element(By.CSS_SELECTOR, f".finMenu[lang='{lang_key}']").get_attribute("class")
    )


def click_excel(driver, out_dir: Path) -> Path:
    toggle = WebDriverWait(driver, 15).until(
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

def parse_ids(args) -> List[str]:
    if args.juristic_ids:
        ids = [s.strip() for s in args.juristic_ids.split(",") if s.strip()]
        if not ids:
            raise SystemExit("รูปแบบ --juristic-ids ไม่ถูกต้อง")
        return ids
    if args.juristic_id:
        return [args.juristic_id]
    if args.ids_file:
        p = Path(args.ids_file)
        if not p.exists():
            raise SystemExit(f"ไม่พบไฟล์: {p}")
        ids: List[str] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#"):
                ids.append(s)
        if not ids:
            raise SystemExit("ไฟล์รายชื่อว่างเปล่า")
        return ids
    raise SystemExit("ต้องระบุ --juristic-id หรือ --juristic-ids หรือ --ids-file")


def write_fs_not_found(out_dir: Path, juristic_id: str) -> Path:
    # เตรียมข้อมูล JSON
    data = {"juristic_id": juristic_id, "result_fs": "not found"}

    # path ของโฟลเดอร์ not_found และสร้างถ้ายังไม่มี
    nf_dir = out_dir / "not_found"
    nf_dir.mkdir(parents=True, exist_ok=True)

    # path ของไฟล์ JSON
    json_path = nf_dir / f"{juristic_id}_financial_result.json"
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # path ของไฟล์ list.txt และบันทึก juristic_id ต่อท้าย
    txt_path = nf_dir / "not_found_list.txt"
    with open(txt_path, "a", encoding="utf-8") as f:
        f.write(f"{juristic_id}\n")

    print(f"บันทึกสถานะงบการเงิน (ไม่พบข้อมูล): {json_path}")
    print(f"เพิ่มรายชื่อใน not_found_list.txt: {juristic_id}")
    return json_path

def run_for_one_company(driver, out_dir: Path, juristic_id: str):
    try:
        scrape_company_title_card(driver, out_dir, juristic_id)
    except Exception as e:
        print(f"[warn] company_title.json: {e}")

    download_company_info_pdf(driver, juristic_id, out_dir)

    # เข้าหน้าข้อมูลงบการเงิน แล้วตัดสินใจว่าจะดาวน์โหลดหรือบันทึก not found
    state = go_financial_tab(driver, out_dir)
    if state == "empty":
        write_fs_not_found(out_dir, juristic_id)
        print("-" * 60)
        print(f"เสร็จสมบูรณ์ (ไม่มีงบการเงิน): {juristic_id}")
        print("-" * 60)
        return

    # มีเมนูรายงาน -> ดาวน์โหลด XLS ทั้งสาม
    download_reports(driver, out_dir, juristic_id)

    print("-" * 60)
    print(f"เสร็จสมบูรณ์: {juristic_id}")
    print("-" * 60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--juristic-id", help="รหัสเดียว")
    ap.add_argument("--juristic-ids", help="หลายรหัส คั่นด้วยจุลภาค เช่น 0105...,0105...,0105...")
    ap.add_argument("--ids-file", help="ระบุไฟล์ .txt ที่มีรายชื่อ juristic id บรรทัดละหนึ่งตัว")
    ap.add_argument("--out-dir", default="./downloads")
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)

    ids = parse_ids(args)

    print("=" * 60)
    print("DBD Financial Scraper (Auto PDF + 3 XLS + Company Title JSON)")
    print("=" * 60)

    driver = make_driver(out_dir, headless=args.headless)
    try:
        # บริษัทแรก: โหลดหน้าและค้นหาด้วยวิธีเดิม
        first_id = ids[0]
        search_by_juristic_id(driver, first_id)
        run_for_one_company(driver, out_dir, first_id)

        # ตัวถัดไป: ใช้ input เดิม ไม่ต้องเข้าเว็บใหม่
        for jid in ids[1:]:
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.3)
            search_via_header_input(driver, jid, out_dir)
            run_for_one_company(driver, out_dir, jid)

        print("=" * 60)
        print("งานครบทุกบริษัทแล้ว")
        print("=" * 60)

    except Exception as e:
        print(f"\nเกิดข้อผิดพลาด: {e}")
        save_debug(driver, "final_error", out_dir)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
