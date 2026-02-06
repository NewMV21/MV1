import os, time, json, gspread, concurrent.futures, re, socket
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

# ✅ Only date comes from MV sheet
DATE_SPREADSHEET_NAME = "MV2 for SQL"
DATE_TAB_NAME = "Sheet15"
DATE_COL_LETTER = "Z"
DATE_SYMBOL_COL = "A"

# ✅ target table
TARGET_TABLE = "nextbaggernew"  # IMPORTANT

MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nextbagger.txt")

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0

# counters
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

DATE_MAP = {}  # symbol -> yyyy-mm-dd


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
    if not val:
        return ""
    s = str(val).strip()
    s = re.sub(r"[^\d/\-]", "", s)
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return ""

def preflight_env_check():
    required = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "GSPREAD_CREDENTIALS", "TRADINGVIEW_COOKIES"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        log(f"❌ PRECHECK: Missing env vars: {missing}")
        return False
    log("✅ PRECHECK: Env vars present")
    return True

def read_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            v = int(open(CHECKPOINT_FILE, "r").read().strip())
            return max(v, -1)
    except:
        pass
    return -1

def write_checkpoint(i):
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(i))
    except:
        pass


# ---------------- DATE MAP ---------------- #
def load_date_map(gc):
    global DATE_MAP
    DATE_MAP = {}

    sym_i = col_letter_to_index(DATE_SYMBOL_COL)
    date_i = col_letter_to_index(DATE_COL_LETTER)

    ss = gc.open(DATE_SPREADSHEET_NAME)
    ws = ss.worksheet(DATE_TAB_NAME)
    values = ws.get_all_values()

    for r in values:
        if len(r) <= max(sym_i, date_i):
            continue
        sym = str(r[sym_i]).strip()
        dt = normalize_date(r[date_i])
        if sym and dt:
            DATE_MAP[sym.upper()] = dt

    log(f"✅ CHECKPOINT: DATE_MAP loaded = {len(DATE_MAP)} from {DATE_SPREADSHEET_NAME}/{DATE_TAB_NAME} (Symbol {DATE_SYMBOL_COL}, Date {DATE_COL_LETTER})")
    log(f"✅ CHECKPOINT: DATE_MAP sample = {list(DATE_MAP.items())[:5]}")


# ---------------- DB ---------------- #
def db_network_diagnostics():
    host = DB_CONFIG.get("host")
    port = DB_CONFIG.get("port", 3306)
    try:
        ip = socket.gethostbyname(host)
        log(f"✅ CHECKPOINT: DB_HOST resolves {host} -> {ip}:{port}")
    except Exception as e:
        log(f"⚠️ CHECKPOINT: DNS resolve failed for {host}: {safe_str(e)}")

def init_db_pool():
    global db_pool

    db_network_diagnostics()

    # 1) Direct connect test (proves real reason)
    try:
        log("🔎 CHECKPOINT: Direct connect test (no pool)...")
        c = mysql.connector.connect(**DB_CONFIG)
        cur = c.cursor()
        cur.execute("SELECT DATABASE()")
        dbname = cur.fetchone()[0]
        cur.close()
        c.close()
        log(f"✅ CHECKPOINT: Direct connect OK (db={dbname})")
    except Exception as e:
        log(f"❌ CHECKPOINT: Direct connect FAILED: {repr(e)}")
        return False

    # 2) Pool with retries (Hostinger sometimes unstable)
    for attempt in range(1, 6):
        try:
            log(f"📡 CHECKPOINT: Connecting to Database pool... attempt={attempt}/5")
            db_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="screenshot_pool",
                pool_size=max(2, MAX_THREADS),
                pool_reset_session=True,
                **DB_CONFIG
            )
            t = db_pool.get_connection()
            t.close()
            log("✅ CHECKPOINT: DATABASE POOL CONNECTION SUCCESSFUL")
            return True
        except Exception as e:
            log(f"❌ CHECKPOINT: POOL CONNECT FAILED attempt {attempt}: {repr(e)}")
            time.sleep(6)

    return False

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if db_pool is None:
        return False
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()

        # confirm DB name (safe)
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]

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

        # confirm row count in THIS table
        cursor.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
        total = cursor.fetchone()[0]

        log(f"✅ DB CONFIRM: host={DB_CONFIG['host']} db={current_db} table={TARGET_TABLE} total_rows_now={total}")

        cursor.close()
        conn.close()
        return True
    except Exception as err:
        log(f"    ❌ DB SAVE ERROR [{symbol}]: {repr(err)}")
        return False


# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()

    # ✅ Chromium binary fix (GitHub runner)
    chrome_bin = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser")
    if not os.path.exists(chrome_bin):
        chrome_bin = "/usr/bin/chromium"
    opts.binary_location = chrome_bin
    log(f"✅ CHECKPOINT: Using Chrome binary = {chrome_bin}")

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,900")

    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter")
    opts.add_argument("--mute-audio")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(45)
    d.implicitly_wait(0)
    return d

def kill_thread_driver():
    try:
        d = getattr(thread_local, "driver", None)
        if d:
            d.quit()
    except:
        pass
    thread_local.driver = None

def force_clear_ads(driver):
    try:
        driver.execute_script("""
            const selectors = [
                "div[class*='overlap-manager']",
                "div[id*='overlap-manager']",
                "div[class*='dialog-']",
                "div[class*='popup-']",
                "div[class*='drawer-']",
                "div[class*='notification-']",
                "[data-role='toast-container']",
                "[role='dialog']"
            ];
            selectors.forEach(sel => {
                document.querySelectorAll(sel).forEach(el => el.remove());
            });
        """)
    except:
        pass

def wait_chart_ready(driver, timeout=20):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
    )

def ensure_thread_driver_logged_in():
    if getattr(thread_local, "driver", None) is None:
        d = get_driver()
        thread_local.driver = d
        with drivers_lock:
            all_drivers.append(d)

        d.get("https://www.tradingview.com/chart/")

        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if cookie_data:
            try:
                cookies = json.loads(cookie_data)
                for c in cookies:
                    d.add_cookie({
                        "name": c.get("name"),
                        "value": c.get("value"),
                        "domain": ".tradingview.com",
                        "path": "/"
                    })
                d.refresh()
                log("✅ CHECKPOINT: Cookies injected and refreshed")
            except Exception as e:
                log(f"⚠️ CHECKPOINT: Cookie load failed: {safe_str(e)}")

    return thread_local.driver

def goto_date_fast(driver, chart_el, target_date):
    ActionChains(driver).move_to_element(chart_el).click().perform()
    time.sleep(0.2)

    ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

    input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
    w = WebDriverWait(driver, 12)
    box = w.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))

    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(Keys.BACKSPACE)
    box.send_keys(target_date)
    box.send_keys(Keys.ENTER)

    time.sleep(0.8)
    force_clear_ads(driver)
    time.sleep(0.6)
    force_clear_ads(driver)


# ---------------- WORKER ---------------- #
def process_row(task):
    global processed_count, skipped_no_date, skipped_bad_row, db_ok, db_fail, selenium_fail

    i, row = task

    try:
        row_clean = {str(k).lower().strip(): v for k, v in row.items()}
        symbol = str(row_clean.get('symbol', '')).strip()
        day_url = str(row_clean.get('day', '')).strip()

        if not symbol or "tradingview.com" not in day_url:
            with progress_lock:
                skipped_bad_row += 1
            log(f"⏭️ SKIP row#{i}: bad row (symbol/url missing) symbol='{symbol}' url='{day_url}'")
            write_checkpoint(i)
            return

        target_date = DATE_MAP.get(symbol.upper(), "")
        if not target_date:
            with progress_lock:
                skipped_no_date += 1
            log(f"⏭️ SKIP row#{i}: {symbol} -> NO DATE in {DATE_SPREADSHEET_NAME}/{DATE_TAB_NAME} col {DATE_COL_LETTER}")
            write_checkpoint(i)
            return

        with progress_lock:
            processed_count += 1
            current_idx = processed_count

        log(f"🚀 START row#{i} [{current_idx}/{total_rows}] {symbol} | date={target_date}")

        try:
            driver = ensure_thread_driver_logged_in()

            log(f"   🌐 GET: {symbol}")
            driver.get(day_url)

            log(f"   📈 WAIT CHART: {symbol}")
            chart = wait_chart_ready(driver, timeout=20)
            force_clear_ads(driver)

            log(f"   🗓️ GOTO DATE: {symbol} -> {target_date}")
            goto_date_fast(driver, chart, target_date)

            log(f"   📸 SCREENSHOT: {symbol}")
            chart = wait_chart_ready(driver, timeout=15)
            force_clear_ads(driver)
            img = chart.screenshot_as_png

        except Exception as se:
            with progress_lock:
                selenium_fail += 1
                db_fail += 1
            log(f"⚠️ SELENIUM ERROR row#{i}: {symbol} -> {safe_str(se)}")
            kill_thread_driver()
            write_checkpoint(i)
            return

        month_val = "Unknown"
        try:
            month_val = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')
        except:
            pass

        ok = save_to_mysql(symbol, "day", img, target_date, month_val)
        with progress_lock:
            if ok:
                db_ok += 1
            else:
                db_fail += 1

        if ok:
            log(f"✅ DB OK row#{i}: inserted/updated {symbol} ({target_date}) -> {TARGET_TABLE}")
        else:
            log(f"❌ DB FAIL row#{i}: {symbol} ({target_date}) -> {TARGET_TABLE}")

        write_checkpoint(i)

    except Exception as e:
        with progress_lock:
            db_fail += 1
        log(f"🔥 FATAL ROW ERROR row#{i}: {safe_str(e)}")
        write_checkpoint(i)
        return


# ---------------- MAIN ---------------- #
def main():
    global total_rows

    log("🏁 CHECKPOINT: Script started")
    log(f"✅ CHECKPOINT: Target table = {TARGET_TABLE}")

    if not preflight_env_check():
        return

    if not init_db_pool():
        return

    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        log("✅ CHECKPOINT: Google credentials loaded")

        load_date_map(gc)

        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()

        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        rows = df.to_dict("records")

        if SHARD_STEP > 1:
            rows = [r for idx, r in enumerate(rows) if (idx % SHARD_STEP) == SHARD_INDEX]

        last_done = read_checkpoint()
        start_from = last_done + 1

        rows_indexed = list(enumerate(rows))
        rows_indexed = [t for t in rows_indexed if t[0] >= start_from]

        total_rows = len(rows_indexed)

        log(f"✅ CHECKPOINT: Main rows loaded = {len(rows)} (raw), to-run = {total_rows} (resume from last_done={last_done})")
        log(f"✅ CHECKPOINT: Sample main row = {rows[0] if rows else 'EMPTY'}")

    except Exception as e:
        log(f"❌ CHECKPOINT: GOOGLE SHEETS ERROR: {repr(e)}")
        return

    if total_rows == 0:
        log("⚠️ CHECKPOINT: Nothing to process (total_rows=0). Check shard/checkpoint.")
        return

    log(f"ℹ️ CHECKPOINT: Starting ThreadPool MAX_THREADS={MAX_THREADS}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, t) for t in rows_indexed]
        for f in concurrent.futures.as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(f"🔥 THREAD CRASH: {repr(e)}")

    with drivers_lock:
        for d in all_drivers:
            try:
                d.quit()
            except:
                pass

    log("\n🏁 COMPLETED.")
    log(f"📊 SUMMARY: processed={processed_count}, db_ok={db_ok}, db_fail={db_fail}, selenium_fail={selenium_fail}, skipped_no_date={skipped_no_date}, skipped_bad_row={skipped_bad_row}")
    log(f"🧾 Checkpoint file used: {CHECKPOINT_FILE}")


if __name__ == "__main__":
    main()
