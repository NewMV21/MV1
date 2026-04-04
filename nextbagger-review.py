import os, time, json, gspread, concurrent.futures
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
import threading

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
DATE_SPREADSHEET_NAME = "MV2 for SQL"
DATE_TAB_NAME = "Sheet2"
TARGET_TABLE = "next_bagger_review_screenshot"
MAX_THREADS = int(os.getenv("MAX_THREADS", "1")) # Set to 1 for stability if RAM is low

db_pool = None
thread_local = threading.local()
DATE_MAP = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- PREP ---------------- #
def load_date_map(gc):
    global DATE_MAP
    try:
        values = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME).get_all_values()
        for r in values:
            if len(r) >= 82:
                sym, dt = str(r[0]).strip().upper(), str(r[81]).strip()
                if sym and dt: DATE_MAP[sym] = dt
        log(f"✅ Loaded {len(DATE_MAP)} dates")
    except Exception as e:
        log(f"❌ Date Map Error: {e}")

def init_db_pool():
    global db_pool
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="pool", pool_size=5,
        host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")), connection_timeout=30
    )

def save_to_mysql(symbol, image, date):
    conn = None
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {TARGET_TABLE} (symbol, screenshot, chart_date) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)", (symbol, image, date))
        conn.commit()
        cursor.close()
    except Exception as e:
        log(f"DB Save Error {symbol}: {e}")
    finally: 
        if conn: conn.close()

# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    # Remove manual binary paths; let Selenium find Chrome automatically
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(45)
    
    try:
        driver.get("https://www.tradingview.com/chart/")
        cookies_str = os.getenv("TRADINGVIEW_COOKIES", "[]")
        cookies = json.loads(cookies_str)
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
    except Exception as e:
        log(f"⚠️ Cookie/Init issue: {str(e)[:50]}")
        
    return driver

# ---------------- WORKER ---------------- #
def process_row(row_data):
    if len(row_data) < 4: return
    symbol, day_url = str(row_data[0]).strip(), str(row_data[3]).strip()
    target_date = DATE_MAP.get(symbol.upper())
    
    if not symbol or not day_url or not target_date: return

    try:
        if not hasattr(thread_local, "driver") or thread_local.driver is None:
            thread_local.driver = get_driver()
        
        d = thread_local.driver
        d.get(day_url)
        
        chart = WebDriverWait(d, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-container")))
        
        # Navigate to Date
        ActionChains(d).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(d).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        box = WebDriverWait(d, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[data-role='datepicker']")))
        box.send_keys(Keys.CONTROL + "a", Keys.BACKSPACE)
        box.send_keys(target_date, Keys.ENTER)
        
        # Final wait for chart update
        time.sleep(8) 
        save_to_mysql(symbol, chart.screenshot_as_png, target_date)
        log(f"✅ {symbol} processed")

    except Exception as e:
        log(f"❌ {symbol} Error: {str(e)[:100]}")
        if hasattr(thread_local, "driver") and thread_local.driver:
            try:
                thread_local.driver.quit()
            except:
                pass
            thread_local.driver = None

# ---------------- MAIN ---------------- #
def main():
    init_db_pool()
    creds = os.getenv("GSPREAD_CREDENTIALS")
    if not creds:
        log("❌ Missing GSPREAD_CREDENTIALS")
        return

    gc = gspread.service_account_from_dict(json.loads(creds))
    load_date_map(gc)

    ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    rows = [r for r in ws.get_all_values()[1:] if r and r[0].strip()]

    log(f"🚀 Starting {len(rows)} tasks with {MAX_THREADS} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(process_row, rows)

    if hasattr(thread_local, "driver") and thread_local.driver:
        thread_local.driver.quit()
    log("🏁 All Finished")

if __name__ == "__main__":
    main()
