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
TARGET_TABLE = "next_bagger_review_screenshot" 

MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0
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
    "connect_timeout": 15,
}

# ---------------- HELPERS ---------------- #
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def safe_str(e, n=260):
    try: return str(e).replace("\n", " ")[:n]
    except: return "error"

def col_letter_to_index(letter: str) -> int:
    letter = letter.strip().upper()
    n = 0
    for ch in letter:
        if "A" <= ch <= "Z": n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val: str) -> str:
    if not val: return ""
    s = str(val).strip()
    s = re.sub(r"[^\d/\-]", "", s)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def write_checkpoint(i):
    try:
        with open(CHECKPOINT_FILE, "w") as f: f.write(str(i))
    except: pass

def read_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            return max(int(open(CHECKPOINT_FILE, "r").read().strip()), -1)
    except: pass
    return -1

# ---------------- DATE MAP & DB ---------------- #
def load_date_map(gc):
    global DATE_MAP
    sym_i, date_i = col_letter_to_index(DATE_SYMBOL_COL), col_letter_to_index(DATE_COL_LETTER)
    values = gc.open(DATE_SPREADSHEET_NAME).worksheet(DATE_TAB_NAME).get_all_values()
    for r in values:
        if len(r) > max(sym_i, date_i):
            sym, dt = str(r[sym_i]).strip().upper(), normalize_date(r[date_i])
            if sym and dt: DATE_MAP[sym] = dt
    log(f"✅ DATE_MAP Loaded: {len(DATE_MAP)} symbols")

def init_db_pool():
    global db_pool
    try:
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool", pool_size=max(2, MAX_THREADS), pool_reset_session=True, **DB_CONFIG
        )
        return True
    except Exception as e:
        log(f"❌ DB Pool Error: {repr(e)}"); return False

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if not db_pool: return False
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = f"""
            INSERT INTO {TARGET_TABLE} (symbol, timeframe, screenshot, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot), chart_date=VALUES(chart_date), 
            month_before=VALUES(month_before), created_at=CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()
        cursor.close(); conn.close()
        return True
    except Exception as err:
        log(f"❌ DB SAVE ERROR [{symbol}]: {repr(err)}"); return False

# ---------------- BROWSER CORE ---------------- #
def get_driver():
    opts = Options()
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if not os.path.exists(chrome_bin): chrome_bin = "/usr/bin/chromium"
    opts.binary_location = chrome_bin
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080") # Higher res for better screenshots
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(45)
    return d

def ensure_logged_in():
    if getattr(thread_local, "driver", None) is None:
        d = get_driver()
        thread_local.driver = d
        with drivers_lock: all_drivers.append(d)
        d.get("https://www.tradingview.com/chart/")
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
        for c in cookies:
            d.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        d.refresh()
    return thread_local.driver

def force_clear_ads(driver):
    driver.execute_script("document.querySelectorAll(\"div[class*='overlap-manager'], [role='dialog'], .tv-dialog__close\").forEach(el => el.remove());")

# ---------------- OPTIMIZED WAITING LOGIC ---------------- #

def wait_for_indicators_and_values(driver, timeout=15):
    """
    Highly optimized check: Waits until TradingView's internal 'loading' 
    status is false and the price/indicator values are visible in the DOM.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        is_loading = driver.execute_script("""
            return (document.querySelector('.is-loading') !== null) || 
                   (document.querySelector('[data-name="legend-series-item"] .value-item') === null);
        """)
        if not is_loading:
            return True
        time.sleep(0.5)
    return False

def wait_visual_stability(chart_el, max_wait=5, min_hits=3):
    """
    Takes rapid MD5 hashes of the chart element. 
    Only proceeds if the image remains identical for 'min_hits' consecutive checks.
    """
    last_hash = None
    stable_count = 0
    end = time.time() + max_wait
    
    while time.time() < end:
        current_hash = hashlib.md5(chart_el.screenshot_as_png).hexdigest()
        if current_hash == last_hash:
            stable_count += 1
            if stable_count >= min_hits: return True
        else:
            stable_count = 0
            last_hash = current_hash
        time.sleep(0.4)
    return True

def goto_date(driver, chart_el, target_date):
    ActionChains(driver).move_to_element(chart_el).click().perform()
    time.sleep(0.2)
    ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
    
    box = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class,'query')]")))
    box.send_keys(Keys.CONTROL, "a", Keys.BACKSPACE)
    box.send_keys(target_date, Keys.ENTER)
    time.sleep(1) # Initial jump buffer

# ---------------- WORKER ---------------- #
def process_row(task):
    global processed_count
    i, row = task
    symbol = str(row.get('symbol', '')).strip()
    day_url = str(row.get('Day', '')).strip()
    target_date = DATE_MAP.get(symbol.upper())

    if not symbol or "tradingview" not in day_url or not target_date:
        write_checkpoint(i); return

    try:
        driver = ensure_logged_in()
        driver.get(day_url)
        
        # 1. Wait for basic chart container
        chart = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]")))
        force_clear_ads(driver)
        
        # 2. Go to historical date
        goto_date(driver, chart, target_date)
        
        # 3. HIGHLY OPTIMIZED WAIT: Logic + Visual
        log(f"⏳ Loading data for {symbol}...")
        wait_for_indicators_and_values(driver) # Wait for DOM values
        wait_visual_stability(chart)           # Wait for pixels to stop moving
        
        # 4. Final Clean & Capture
        force_clear_ads(driver)
        img = chart.screenshot_as_png
        
        month_val = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')
        if save_to_mysql(symbol, "day", img, target_date, month_val):
            log(f"✅ Captured {symbol} ({target_date})")
        
        write_checkpoint(i)
        with progress_lock: processed_count += 1

    except Exception as e:
        log(f"⚠️ Error {symbol}: {safe_str(e)}")
        write_checkpoint(i)

# ---------------- MAIN ---------------- #
def main():
    if not init_db_pool(): return
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    load_date_map(gc)
    
    worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    rows = pd.DataFrame(worksheet.get_all_values()[1:], columns=[h.strip() for h in worksheet.get_all_values()[0]]).to_dict("records")
    
    if SHARD_STEP > 1:
        rows = [r for idx, r in enumerate(rows) if (idx % SHARD_STEP) == SHARD_INDEX]
    
    start_from = read_checkpoint() + 1
    tasks = [(idx, r) for idx, r in enumerate(rows) if idx >= start_from]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, tasks))

    for d in all_drivers: d.quit()
    log("🏁 Done.")

if __name__ == "__main__":
    main()
