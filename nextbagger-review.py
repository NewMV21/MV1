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
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

progress_lock = threading.Lock()
processed_count = 0
db_pool = None
thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []
DATE_MAP = {}

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# ---------------- HELPERS ---------------- #
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def col_letter_to_index(letter):
    letter = letter.upper()
    n = 0
    for ch in letter:
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val):
    if not val: return ""
    s = str(val).strip()
    s = re.sub(r"[^\d/\-]", "", s)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return ""

def write_checkpoint(i):
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i))
    except:
        pass

def read_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            return int(open(CHECKPOINT_FILE).read().strip())
    except:
        pass
    return -1

# ---------------- LOAD DATE MAP ---------------- #
def load_date_map(gc):
    global DATE_MAP

    sym_i = col_letter_to_index(DATE_SYMBOL_COL)
    date_i = col_letter_to_index(DATE_COL_LETTER)

    values = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME).get_all_values()

    for r in values:
        if len(r) > max(sym_i, date_i):
            sym = str(r[sym_i]).strip().upper()
            dt = normalize_date(r[date_i])
            if sym and dt:
                DATE_MAP[sym] = dt

    log(f"✅ DATE_MAP Loaded: {len(DATE_MAP)} symbols")

# ---------------- DB ---------------- #
def init_db_pool():
    global db_pool
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="pool",
        pool_size=max(2, MAX_THREADS),
        **DB_CONFIG
    )

def save_to_mysql(symbol, image, date):
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {TARGET_TABLE} (symbol, screenshot, chart_date)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)
        """, (symbol, image, date))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        log(f"DB Error: {e}")

# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(options=opts)

def ensure_logged_in():
    if not hasattr(thread_local, "driver"):
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

# ---------------- WORKER ---------------- #
def process_row(task):
    i, row = task

    values = list(row.values())

    if len(values) < 4:
        return

    symbol = str(values[0]).strip()   # Column A
    day_url = str(values[3]).strip()  # Column D

    target_date = DATE_MAP.get(symbol.upper())

    log(f"➡ {symbol} | {target_date}")

    if not symbol or not day_url or not target_date:
        return

    try:
        driver = ensure_logged_in()
        driver.get(day_url)

        chart = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
        )

        # Go to date
        ActionChains(driver).move_to_element(chart).click().perform()
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

        box = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, "//input"))
        )
        box.send_keys(target_date, Keys.ENTER)

        time.sleep(2)

        img = chart.screenshot_as_png

        save_to_mysql(symbol, img, target_date)

        log(f"✅ Captured {symbol}")

        write_checkpoint(i)

    except Exception as e:
        log(f"❌ Error {symbol}: {e}")

# ---------------- MAIN ---------------- #
def main():
    init_db_pool()

    gc = gspread.service_account_from_dict(json.loads(os.getenv("GSPREAD_CREDENTIALS")))

    load_date_map(gc)

    worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    values = worksheet.get_all_values()

    rows = pd.DataFrame(values[1:], columns=values[0]).to_dict("records")

    start = read_checkpoint() + 1
    tasks = [(i, r) for i, r in enumerate(rows) if i >= start]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(process_row, tasks)

    log("🏁 Done.")

if __name__ == "__main__":
    main()
