import os, time, json, gspread, concurrent.futures, re, hashlib
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
DATE_SYMBOL_COL = "A"
DATE_COL_LETTER = "CD"
TARGET_TABLE = "next_bagger_review_screenshot"
MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))

db_pool = None
thread_local = threading.local()
DATE_MAP = {}

# ---------------- HELPERS ---------------- #
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def col_letter_to_index(letter):
    n = 0
    for ch in letter.upper():
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val):
    if not val: return ""
    s = re.sub(r"[^\d/\-]", "", str(val))
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return ""

# ---------------- DATE MAP ---------------- #
def load_date_map(gc):
    global DATE_MAP
    sym_i = col_letter_to_index(DATE_SYMBOL_COL)
    date_i = col_letter_to_index(DATE_COL_LETTER)
    try:
        values = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME).get_all_values()
        for r in values:
            if len(r) > max(sym_i, date_i):
                sym = str(r[sym_i]).strip().upper()
                dt = normalize_date(r[date_i])
                if sym and dt:
                    DATE_MAP[sym] = dt
        log(f"✅ DATE_MAP Loaded: {len(DATE_MAP)} symbols")
    except Exception as e:
        log(f"❌ Error loading Date Map: {e}")

# ---------------- DB ---------------- #
def init_db_pool():
    global db_pool
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="pool",
        pool_size=max(2, MAX_THREADS + 2), # Buffer for pool size
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306"))
    )

def save_to_mysql(symbol, image, date):
    conn = None
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = f"""
            INSERT INTO {TARGET_TABLE} (symbol, screenshot, chart_date)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)
        """
        cursor.execute(query, (symbol, image, date))
        conn.commit()
        cursor.close()
    except Exception as e:
        log(f"DB Error for {symbol}: {e}")
    finally:
        if conn:
            conn.close() # Return to pool

# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if not os.path.exists(chrome_bin):
        chrome_bin = "/usr/bin/chromium"
    
    opts.binary_location = chrome_bin
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=opts)

def ensure_logged_in():
    # If driver exists but is dead, clean it up
    if hasattr(thread_local, "driver"):
        try:
            thread_local.driver.title
        except:
            thread_local.driver = None

    if not hasattr(thread_local, "driver") or thread_local.driver is None:
        d = get_driver()
        thread_local.driver = d
        d.get("https://www.tradingview.com/chart/")
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
        for c in cookies:
            d.add_cookie({
                "name": c["name"],
                "value": c["value"],
                "domain": ".tradingview.com",
                "path": "/"
            })
        d.refresh()
    return thread_local.driver

# ---------------- SMART WAIT ---------------- #
def wait_for_chart_ready(driver, timeout=20):
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Check if TV loading indicator is gone
            loading = driver.execute_script("return document.querySelector('.is-loading') !== null;")
            # Check if any chart values are actually rendered
            value_present = driver.execute_script("return document.querySelector('[data-name=\"legend-series-item\"] .value-item') !== null;")
            # Ensure "Go To" dialog is CLOSED (it blocks screenshots)
            dialog_open = driver.execute_script("return document.querySelector('[data-name=\"go-to-date-dialog\"]') !== null;")
            
            if not loading and value_present and not dialog_open:
                return True
        except:
            pass
        time.sleep(1)
    return False

# ---------------- WORKER ---------------- #
def process_row(task):
    i, row = task
    values = list(row.values())
    if len(values) < 4: return

    symbol = str(values[0]).strip()
    day_url = str(values[3]).strip()
    target_date = DATE_MAP.get(symbol.upper())

    if not symbol or not day_url or not target_date:
        return

    log(f"➡ Starting {symbol} for date {target_date}")
    
    try:
        driver = ensure_logged_in()
        driver.get(day_url)

        # Wait for base container
        chart = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
        )

        # Open Go-To Date
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

        # Fill date
        box = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "input[data-role='datepicker']"))
        )
        box.send_keys(Keys.CONTROL + "a")
        box.send_keys(Keys.BACKSPACE)
        time.sleep(0.5)
        box.send_keys(target_date, Keys.ENTER)

        # Wait for data load and for the popup to disappear
        if wait_for_chart_ready(driver):
            time.sleep(2) # Final buffer for rendering
            img = chart.screenshot_as_png
            save_to_mysql(symbol, img, target_date)
            log(f"✅ Success: {symbol}")
        else:
            log(f"⚠️ Timeout: Chart for {symbol} never fully rendered.")

    except Exception as e:
        log(f"❌ Error {symbol}: {str(e)[:100]}")
        # If the browser is broken, kill it so it restarts for the next symbol
        if "session" in str(e).lower() or "reachable" in str(e).lower():
            if hasattr(thread_local, "driver") and thread_local.driver:
                try: thread_local.driver.quit()
                except: pass
                thread_local.driver = None

# ---------------- MAIN ---------------- #
def main():
    init_db_pool()
    
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if not creds_json:
        log("❌ GSPREAD_CREDENTIALS env var not found!")
        return

    gc = gspread.service_account_from_dict(json.loads(creds_json))
    load_date_map(gc)

    worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    values = worksheet.get_all_values()
    if not values: return

    rows = pd.DataFrame(values[1:], columns=values[0]).to_dict("records")
    tasks = list(enumerate(rows))

    log(f"🚀 Processing {len(tasks)} symbols across {MAX_THREADS} threads")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Wrap in list to force execution
        list(executor.map(process_row, tasks))

    # Final Cleanup
    if hasattr(thread_local, "driver") and thread_local.driver:
        thread_local.driver.quit()

    log("🏁 Done.")

if __name__ == "__main__":
    main()
