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
DAY_URL_COLUMN_NAME = "Day" 

# ✅ Date Source (MV2 for SQL)
DATE_SPREADSHEET_NAME = "MV2 for SQL"
DATE_TAB_NAME = "Sheet2"
DATE_COL_LETTER = "CD"
DATE_SYMBOL_COL = "A"

TARGET_TABLE = "next_bagger_review_screenshot"

MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

BROWSER_RESTART_LIMIT = 40 

progress_lock = threading.Lock()
processed_count = 0
db_ok = 0
db_fail = 0
skipped_no_date = 0

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 30,
}

db_pool = None
thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []
DATE_MAP = {}

# ---------------- HELPERS ---------------- #
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def col_letter_to_index(letter: str) -> int:
    n = 0
    for ch in letter.strip().upper():
        if "A" <= ch <= "Z": n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val: str) -> str:
    if not val: return ""
    s = re.sub(r"[^\d/\-]", "", str(val).strip())
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def write_checkpoint(i):
    try:
        with open(CHECKPOINT_FILE, "w") as f: f.write(str(i))
    except: pass

def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try: 
            with open(CHECKPOINT_FILE, "r") as f:
                c = f.read().strip()
                return int(c) if c else -1
        except: return -1
    return -1

# ---------------- CORE ---------------- #
def load_date_map(gc):
    global DATE_MAP
    DATE_MAP = {}
    ws = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME)
    vals = ws.get_all_values()
    s_i, d_i = col_letter_to_index(DATE_SYMBOL_COL), col_letter_to_index(DATE_COL_LETTER)
    for r in vals:
        if len(r) > max(s_i, d_i):
            s, d = r[s_i].strip().upper(), normalize_date(r[d_i])
            if s and d: DATE_MAP[s] = d
    log(f"✅ DATE_MAP: Loaded {len(DATE_MAP)} valid dates from {DATE_TAB_NAME}")

def init_db_pool():
    global db_pool
    try:
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool", pool_size=MAX_THREADS + 2, **DB_CONFIG
        )
        return True
    except Exception as e:
        log(f"❌ DB Pool Error: {e}")
        return False

def save_to_mysql(symbol, timeframe, img, chart_date, month):
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = f"INSERT INTO {TARGET_TABLE} (symbol, timeframe, screenshot, chart_date, month_before) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot), chart_date=VALUES(chart_date), month_before=VALUES(month_before)"
        cursor.execute(query, (symbol, timeframe, img, chart_date, month))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        log(f"    ❌ DB Error {symbol}: {e}")
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if os.path.exists(chrome_bin): opts.binary_location = chrome_bin
    d = webdriver.Chrome(options=opts)
    return d

def process_row(task):
    global processed_count, db_ok, db_fail, skipped_no_date
    idx, row = task
    row_clean = {str(k).strip(): v for k, v in row.items()}
    symbol = str(row_clean.get('symbol', '')).strip()
    day_url = str(row_clean.get(DAY_URL_COLUMN_NAME, '')).strip()
    
    # ✅ Only proceed if symbol has a valid date
    target_date = DATE_MAP.get(symbol.upper())

    if not target_date:
        with progress_lock: skipped_no_date += 1
        write_checkpoint(idx)
        return

    if "tradingview.com" not in day_url:
        write_checkpoint(idx)
        return

    if not hasattr(thread_local, "counter"): thread_local.counter = 0

    try:
        if not hasattr(thread_local, "driver") or thread_local.driver is None or thread_local.counter >= BROWSER_RESTART_LIMIT:
            if hasattr(thread_local, "driver") and thread_local.driver:
                try: thread_local.driver.quit()
                except: pass
            thread_local.driver = get_driver()
            thread_local.counter = 0
            with drivers_lock: all_drivers.append(thread_local.driver)
            thread_local.driver.get("https://www.tradingview.com/")
            for c in json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]")):
                try: thread_local.driver.add_cookie(c)
                except: pass
            thread_local.driver.refresh()

        d = thread_local.driver
        thread_local.counter += 1
        d.get(day_url)
        
        wait = WebDriverWait(d, 35)
        chart = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]")))
        
        # Navigate to Date
        ActionChains(d).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(d).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        box = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class,'input')]")))
        box.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
        box.send_keys(target_date, Keys.ENTER)
        
        time.sleep(9) # Extra wait for all indicators to render
        
        # UI Cleanup
        d.execute_script("document.querySelectorAll('.overlap-manager-root, .tv-dialog__close, [role=\"dialog\"], .tv-toast').forEach(el => el.remove());")
        
        img = chart.screenshot_as_png
        month = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')

        if save_to_mysql(symbol, "day", img, target_date, month):
            with progress_lock: 
                db_ok += 1
                processed_count += 1
            log(f"✅ [{processed_count}] Saved {symbol} for {target_date}")
        else:
            with progress_lock: db_fail += 1
            
        write_checkpoint(idx)

    except Exception as e:
        log(f"🔥 Error {symbol}: {str(e)[:100]}")
        try: thread_local.driver.quit()
        except: pass
        thread_local.driver = None
        write_checkpoint(idx)

def main():
    if not init_db_pool(): return
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        load_date_map(gc)

        ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
        all_rows = ws.get_all_records()
        
        start = read_checkpoint() + 1
        # ✅ Filter tasks that actually have a date in the map
        tasks = []
        for i, r in enumerate(all_rows):
            if i >= start:
                sym = str(r.get('symbol', '')).strip().upper()
                if sym in DATE_MAP:
                    if (i % SHARD_STEP) == SHARD_INDEX:
                        tasks.append((i, r))

        log(f"🚀 Processing {len(tasks)} symbols with valid dates (Resuming from {start})")
        
        # ✅ FORCED EXECUTION: list() ensures the map is fully iterated
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            list(executor.map(process_row, tasks))

    except Exception as e:
        log(f"❌ Fatal: {e}")
    finally:
        with drivers_lock:
            for d in all_drivers:
                try: d.quit()
                except: pass
        log(f"📊 Final Stats: Success={db_ok}, Fail={db_fail}, Skipped(No Date)={skipped_no_date}")

if __name__ == "__main__":
    main()
