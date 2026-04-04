import os, time, json, gspread, concurrent.futures, re
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))

db_pool = None
thread_local = threading.local()
DATE_MAP = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- DATE & DB ---------------- #
def load_date_map(gc):
    global DATE_MAP
    try:
        # CD is column 82 (index 81)
        values = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME).get_all_values()
        for r in values:
            if len(r) >= 82:
                sym = str(r[0]).strip().upper()
                dt = str(r[81]).strip()
                if sym and dt: DATE_MAP[sym] = dt
        log(f"✅ Loaded {len(DATE_MAP)} dates from Date Map")
    except Exception as e: log(f"❌ Date Map Error: {e}")

def init_db_pool():
    global db_pool
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="pool",
        pool_size=max(2, MAX_THREADS + 1),
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")),
        connection_timeout=20
    )

def save_to_mysql(symbol, image, date):
    conn = None
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {TARGET_TABLE} (symbol, screenshot, chart_date) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)", (symbol, image, date))
        conn.commit()
        cursor.close()
    finally:
        if conn: conn.close()

# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if os.path.exists(chrome_bin): opts.binary_location = chrome_bin
    
    driver = webdriver.Chrome(options=opts)
    driver.get("https://www.tradingview.com/chart/")
    
    try:
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
    except:
        log("⚠️ Cookie warning: Continuing without cookies")
    
    return driver

# ---------------- WORKER ---------------- #
def process_row(row_data):
    if len(row_data) < 4: return

    symbol = str(row_data[0]).strip()
    day_url = str(row_data[3]).strip() # Column D
    target_date = DATE_MAP.get(symbol.upper())

    if not symbol or not day_url or not target_date:
        return

    try:
        if not hasattr(thread_local, "driver"): 
            thread_local.driver = get_driver()
        
        driver = thread_local.driver
        driver.get(day_url)
        
        # Wait for chart
        chart = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
        )
        
        # Go to Date
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        input_box = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "input[data-role='datepicker']"))
        )
        input_box.send_keys(Keys.CONTROL + "a", Keys.BACKSPACE)
        input_box.send_keys(target_date, Keys.ENTER)
        
        # Allow time for jump and render
        time.sleep(6) 
        
        img = chart.screenshot_as_png
        save_to_mysql(symbol, img, target_date)
        log(f"✅ Captured: {symbol}")

    except Exception as e:
        log(f"❌ Error {symbol}: {str(e)[:100]}")

# ---------------- MAIN ---------------- #
def main():
    init_db_pool()
    gc = gspread.service_account_from_dict(json.loads(os.getenv("GSPREAD_CREDENTIALS")))
    load_date_map(gc)

    ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    all_values = ws.get_all_values()
    
    # Filter out header and empty rows
    data_rows = [r for r in all_values[1:] if len(r) > 0 and r[0].strip()]
    
    if not data_rows:
        log("❌ No data found in the spreadsheet!")
        return

    log(f"🚀 Processing {len(data_rows)} rows using {MAX_THREADS} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(process_row, data_rows)

    if hasattr(thread_local, "driver"):
        thread_local.driver.quit()
    
    log("🏁 Done.")

if __name__ == "__main__":
    main()
