import os, time, json, gspread, concurrent.futures, re, socket, hashlib
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
DATE_COL_LETTER = "CD"
DATE_SYMBOL_COL = "A"

TARGET_TABLE = "next_bag_review_screenshot"

MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0

skipped_no_date = 0
skipped_bad_row = 0
db_ok = 0
db_fail = 0
selenium_fail = 0

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 15,
}

db_pool = None
thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []
DATE_MAP = {}

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(e, n=260):
    try:
        return str(e).replace("\n", " ")[:n]
    except:
        return "error"

def col_letter_to_index(letter: str) -> int:
    letter = letter.strip().upper()
    n = 0
    for ch in letter:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val: str) -> str:
    if not val: return ""
    s = str(val).strip()
    s = re.sub(r"[^\d/\-]", "", s)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def preflight_env_check():
    required = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "GSPREAD_CREDENTIALS", "TRADINGVIEW_COOKIES"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        log(f"❌ PRECHECK: Missing env vars: {missing}")
        return False
    return True

def read_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            v = int(open(CHECKPOINT_FILE, "r").read().strip())
            return max(v, -1)
    except: pass
    return -1

def write_checkpoint(i):
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i))
    except: pass

def load_date_map(gc):
    global DATE_MAP
    DATE_MAP = {}
    sym_i = col_letter_to_index(DATE_SYMBOL_COL)
    date_i = col_letter_to_index(DATE_COL_LETTER)
    ss = gc.open(DATE_SPREADSHEET_NAME)
    ws = ss.worksheet(DATE_TAB_NAME)
    values = ws.get_all_values()
    for r in values:
        if len(r) <= max(sym_i, date_i): continue
        sym = str(r[sym_i]).strip()
        dt = normalize_date(r[date_i])
        if sym and dt:
            DATE_MAP[sym.upper()] = dt

def init_db_pool():
    global db_pool
    try:
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool",
            pool_size=max(2, MAX_THREADS + 1),
            pool_reset_session=True,
            **DB_CONFIG
        )
        return True
    except Exception as e:
        log(f"❌ POOL CONNECT FAILED: {repr(e)}")
        return False

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if db_pool is None: return False
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = f"""
            INSERT INTO {TARGET_TABLE} (symbol, timeframe, screenshot, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as err:
        log(f"    ❌ DB SAVE ERROR [{symbol} {timeframe}]: {repr(err)}")
        return False

# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if not os.path.exists(chrome_bin): chrome_bin = "/usr/bin/chromium"
    opts.binary_location = chrome_bin
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080") # Increased window size
    opts.add_argument("--disable-blink-features=AutomationControlled")
    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(45)
    return d

def ensure_thread_driver_logged_in():
    if getattr(thread_local, "driver", None) is None:
        d = get_driver()
        thread_local.driver = d
        with drivers_lock: all_drivers.append(d)
        d.get("https://www.tradingview.com/chart/")
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if cookie_data:
            cookies = json.loads(cookie_data)
            for c in cookies:
                d.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
            d.refresh()
            time.sleep(2)
    return thread_local.driver

def wait_chart_ready(driver, timeout=25):
    WebDriverWait(driver, timeout).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
    # Wait for the chart canvas to be present
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]")))

def force_clear_ads(driver):
    try:
        # Aggressive cleanup of dialogs, overlays, and sidebars that might block the screenshot
        script = """
        const selectors = [
            "div[class*='overlap-manager']", 
            "[role='dialog']", 
            "div[class*='dialog']", 
            ".tv-dialog__modal-body",
            "div[class*='toast']",
            "div[class*='notification']"
        ];
        selectors.forEach(s => {
            document.querySelectorAll(s).forEach(el => el.remove());
        });
        """
        driver.execute_script(script)
    except: pass

def wait_chart_stable_for_screenshot(driver, chart_el, max_wait=8.0):
    end = time.time() + max_wait
    last_h = None
    stable_hits = 0
    while time.time() < end:
        try:
            png = chart_el.screenshot_as_png
            h = hashlib.md5(png).hexdigest()
            if h == last_h:
                stable_hits += 1
                if stable_hits >= 3: return True # Increased stability requirement
            else:
                stable_hits = 0
                last_h = h
        except: return False
        time.sleep(0.5)
    return True

def force_timeframe(driver, tf_key):
    try:
        actions = ActionChains(driver)
        actions.send_keys(tf_key).perform()
        time.sleep(0.8)
        actions.send_keys(Keys.ENTER).perform()
        time.sleep(2.5) # Increased wait for candles to reload
    except: pass

def goto_date_fast(driver, chart_el, target_date):
    # Ensure chart is focused
    ActionChains(driver).move_to_element(chart_el).click().perform()
    time.sleep(0.5)
    
    # Open "Go to" dialog
    ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
    
    input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
    box = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
    
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(Keys.BACKSPACE)
    box.send_keys(target_date)
    time.sleep(0.5)
    box.send_keys(Keys.ENTER)
    
    # CRITICAL: Wait for the dialog to actually disappear before moving on
    try:
        WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.XPATH, input_xpath)))
    except:
        force_clear_ads(driver)
    
    time.sleep(2.0) # Buffer for chart jump

# ---------------- WORKER ---------------- #
def process_row(task):
    global processed_count, skipped_no_date, skipped_bad_row, db_ok, db_fail, selenium_fail
    i, row = task
    try:
        row_clean = {str(k).lower().strip(): v for k, v in row.items()}
        symbol = str(row_clean.get('symbol', '')).strip()
        
        day_url = str(row_clean.get('day', '')).strip()
        week_url = str(row_clean.get('week', '')).strip()

        if not symbol or not day_url or not week_url:
            with progress_lock: skipped_bad_row += 1
            write_checkpoint(i)
            return

        target_date = DATE_MAP.get(symbol.upper(), "")
        if not target_date:
            with progress_lock: skipped_no_date += 1
            write_checkpoint(i)
            return

        driver = ensure_thread_driver_logged_in()

        timeframe_tasks = [
            ("day", day_url, "1D"),
            ("week", week_url, "1W")
        ]

        for tf_label, tf_url, tf_key in timeframe_tasks:
            log(f"🚀 ROW#{i} | {symbol} | {tf_label.upper()}")
            driver.get(tf_url)
            
            chart = wait_chart_ready(driver)
            force_clear_ads(driver)

            # 1. Force the timeframe
            force_timeframe(driver, tf_key)

            # 2. Go to the date
            goto_date_fast(driver, chart, target_date)
            
            # 3. Final cleanup and stabilization
            force_clear_ads(driver)
            wait_chart_stable_for_screenshot(driver, chart)
            
            # Take screenshot
            img = chart.screenshot_as_png

            # Save to DB
            month_val = "Unknown"
            try: month_val = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')
            except: pass

            ok = save_to_mysql(symbol, tf_label, img, target_date, month_val)
            with progress_lock:
                if ok: db_ok += 1
                else: db_fail += 1

        with progress_lock: processed_count += 1
        write_checkpoint(i)

    except Exception as e:
        log(f"🔥 ERROR row#{i}: {safe_str(e)}")
        write_checkpoint(i)

def main():
    global total_rows
    if not preflight_env_check() or not init_db_pool(): return
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        load_date_map(gc)
        
        ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
        all_vals = ws.get_all_values()
        df = pd.DataFrame(all_vals[1:], columns=[h.strip() for h in all_vals[0]])
        rows = df.loc[:, ~df.columns.duplicated()].to_dict("records")

        if SHARD_STEP > 1:
            rows = [r for idx, r in enumerate(rows) if (idx % SHARD_STEP) == SHARD_INDEX]

        start_from = read_checkpoint() + 1
        rows_indexed = [t for t in list(enumerate(rows)) if t[0] >= start_from]
        total_rows = len(rows_indexed)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            list(executor.map(process_row, rows_indexed))

    finally:
        with drivers_lock:
            for d in all_drivers: d.quit()

if __name__ == "__main__":
    main()
