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
DAY_URL_COLUMN_NAME = "Day"  # ✅ Updated to your new column name

# ✅ Date Source Config (Updated to sheet2 and Col CD)
DATE_SPREADSHEET_NAME = "MV2 for SQL"
DATE_TAB_NAME = "Sheet2"
DATE_COL_LETTER = "CD"
DATE_SYMBOL_COL = "A"

# ✅ Target Table
TARGET_TABLE = "next_bagger_review_screenshot"

MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0
db_ok = 0
db_fail = 0

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

def col_letter_to_index(letter: str) -> int:
    letter = letter.strip().upper()
    n = 0
    for ch in letter:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
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
        try: return int(open(CHECKPOINT_FILE, "r").read().strip())
        except: return -1
    return -1

# ---------------- CORE LOGIC ---------------- #
def load_date_map(gc):
    global DATE_MAP
    DATE_MAP = {}
    ws = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME)
    values = ws.get_all_values()
    sym_i = col_letter_to_index(DATE_SYMBOL_COL)
    date_i = col_letter_to_index(DATE_COL_LETTER)

    for r in values:
        if len(r) > max(sym_i, date_i):
            sym = str(r[sym_i]).strip().upper()
            dt = normalize_date(r[date_i])
            if sym and dt: DATE_MAP[sym] = dt
    log(f"✅ DATE_MAP: Loaded {len(DATE_MAP)} symbols from {DATE_TAB_NAME} (Col {DATE_COL_LETTER})")

def init_db_pool():
    global db_pool
    try:
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool", pool_size=max(2, MAX_THREADS + 1), **DB_CONFIG
        )
        return True
    except Exception as e:
        log(f"❌ DB Pool Error: {e}")
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
        log(f"    ❌ DB ERROR [{symbol}]: {err}")
        return False

# ---------------- BROWSER & WORKER ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    d = webdriver.Chrome(options=opts)
    return d

def process_row(task):
    global processed_count, db_ok, db_fail
    i, row = task
    # Clean keys to match column name exactly
    row_clean = {str(k).strip(): v for k, v in row.items()}
    symbol = str(row_clean.get('symbol', '')).strip()
    day_url = str(row_clean.get(DAY_URL_COLUMN_NAME, '')).strip()

    target_date = DATE_MAP.get(symbol.upper(), "")
    if not target_date or "tradingview.com" not in day_url:
        write_checkpoint(i)
        return

    try:
        if not hasattr(thread_local, "driver"):
            thread_local.driver = get_driver()
            with drivers_lock: all_drivers.append(thread_local.driver)
            # Inject cookies for login session
            thread_local.driver.get("https://www.tradingview.com/")
            cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
            for c in cookies: thread_local.driver.add_cookie(c)

        d = thread_local.driver
        d.get(day_url)
        
        # Wait for chart
        wait = WebDriverWait(d, 20)
        chart = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]")))
        
        # Go to date
        ActionChains(d).move_to_element(chart).click().perform()
        time.sleep(0.5)
        ActionChains(d).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        box = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class,'input')]")))
        box.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
        box.send_keys(target_date, Keys.ENTER)
        
        time.sleep(4) # Wait for indicators to load
        img = chart.screenshot_as_png
        month_val = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')

        if save_to_mysql(symbol, "day", img, target_date, month_val):
            with progress_lock: db_ok += 1
            log(f"✅ row#{i} {symbol} Saved")
        else:
            with progress_lock: db_fail += 1
            
        write_checkpoint(i)
    except Exception as e:
        log(f"🔥 row#{i} Error: {str(e)[:100]}")

def main():
    if not init_db_pool(): return
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    load_date_map(gc)

    ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    all_rows = ws.get_all_records()
    
    # Resuming and Sharding logic
    start_idx = read_checkpoint() + 1
    tasks = [(idx, r) for idx, r in enumerate(all_rows) if idx >= start_idx]
    if SHARD_STEP > 1:
        tasks = [t for t in tasks if (t[0] % SHARD_STEP) == SHARD_INDEX]

    log(f"🚀 Starting processing {len(tasks)} tasks...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, tasks))

    for d in all_drivers: d.quit()
    log("🏁 Done.")

if __name__ == "__main__":
    main()
