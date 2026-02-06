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
# MAIN SHEET (Symbol + URL)
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

# DATE SHEET (Date in Column Z)
DATE_SPREADSHEET_NAME = "MV2 for SQL"
DATE_TAB_NAME = "Sheet15"
DATE_SYMBOL_COL_LETTER = "A"   # assumed Symbol is in Column A in Sheet15
DATE_DATE_COL_LETTER   = "Z"   # Date is in Column Z in Sheet15

# ✅ For GitHub runner / VPS: 2 is usually fastest & stable
MAX_THREADS = int(os.getenv("MAX_THREADS", "2"))

# Optional sharding
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 30
}

db_pool = None
thread_local = threading.local()
drivers_lock = threading.Lock()
all_drivers = []

DATE_MAP = {}  # symbol -> yyyy-mm-dd


# ---------------- SMALL HELPERS ---------------- #
def col_letter_to_index(letter: str) -> int:
    """A->0, B->1, ..., Z->25, AA->26 ..."""
    letter = letter.strip().upper()
    n = 0
    for ch in letter:
        if not ("A" <= ch <= "Z"):
            continue
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def normalize_date(val: str) -> str:
    """
    Try to normalize date into YYYY-MM-DD.
    Accepts:
      - YYYY-MM-DD (already)
      - YYYY/MM/DD
      - DD-MM-YYYY
      - DD/MM/YYYY
    If can't parse, returns original trimmed string.
    """
    if not val:
        return ""
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)

    # already YYYY-MM-DD
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except:
        pass

    # YYYY/MM/DD
    try:
        dt = datetime.strptime(s, "%Y/%m/%d")
        return dt.strftime("%Y-%m-%d")
    except:
        pass

    # DD-MM-YYYY
    try:
        dt = datetime.strptime(s, "%d-%m-%Y")
        return dt.strftime("%Y-%m-%d")
    except:
        pass

    # DD/MM/YYYY
    try:
        dt = datetime.strptime(s, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except:
        pass

    return s


# ---------------- DB ---------------- #
def init_db_pool():
    global db_pool
    try:
        print("📡 FLAG: Connecting to Database...")
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool",
            pool_size=MAX_THREADS + 2,
            **DB_CONFIG
        )
        print("✅ FLAG: DATABASE CONNECTION SUCCESSFUL")
        return True
    except Exception as e:
        print(f"❌ FLAG: DATABASE CONNECTION FAILED: {e}")
        return False


def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if db_pool is None:
        return False
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO next_bagger_review_screenshot
                (symbol, timeframe, screenshot, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot = VALUES(screenshot),
                month_before = VALUES(month_before),
                updated_at = CURRENT_TIMESTAMP
        """

        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()

        cursor.close()
        conn.close()
        return True
    except Exception as err:
        print(f"    ❌ DB SAVE ERROR [{symbol}] -> {err}")
        return False


# ---------------- BROWSER ---------------- #
def get_driver():
    opts = Options()

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

    # ✅ block heavy resources (images/fonts)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(35)
    d.implicitly_wait(0)
    return d


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


def wait_chart_ready(driver, timeout=18):
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
            except Exception as e:
                print(f"⚠️ FLAG: Cookie load failed: {e}")

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


# ---------------- DATE MAP LOADER ---------------- #
def load_date_map(gc):
    """
    Loads symbol->date from:
      Spreadsheet: MV2 for SQL
      Sheet: Sheet15
      Symbol column: A
      Date column: Z
    """
    global DATE_MAP
    DATE_MAP = {}

    try:
        ss = gc.open(DATE_SPREADSHEET_NAME)
        ws = ss.worksheet(DATE_TAB_NAME)
        values = ws.get_all_values()

        sym_i = col_letter_to_index(DATE_SYMBOL_COL_LETTER)
        date_i = col_letter_to_index(DATE_DATE_COL_LETTER)

        for r in values:
            if len(r) <= max(sym_i, date_i):
                continue
            sym = str(r[sym_i]).strip()
            dt  = normalize_date(r[date_i])
            if sym and dt:
                DATE_MAP[sym.upper()] = dt

        print(f"✅ FLAG: DATE MAP LOADED -> {len(DATE_MAP)} symbols from '{DATE_SPREADSHEET_NAME}' / '{DATE_TAB_NAME}' (col {DATE_DATE_COL_LETTER})")
    except Exception as e:
        print(f"❌ FLAG: DATE MAP LOAD FAILED: {e}")
        DATE_MAP = {}


# ---------------- WORKER ---------------- #
def process_row(row):
    global processed_count

    row_clean = {str(k).lower().strip(): v for k, v in row.items()}
    symbol = str(row_clean.get('symbol', '')).strip()
    day_url = str(row_clean.get('day', '')).strip()

    if not symbol or "tradingview.com" not in day_url:
        return

    # date from DATE_MAP using symbol
    target_date = DATE_MAP.get(symbol.upper(), "")
    if not target_date:
        print(f"⏭️ FLAG: SKIP {symbol} -> No date found in {DATE_SPREADSHEET_NAME}/{DATE_TAB_NAME} col {DATE_DATE_COL_LETTER}")
        return

    with progress_lock:
        processed_count += 1
        current_idx = processed_count
        if current_idx == 1:
            print(f"ℹ️ FLAG: Threads={MAX_THREADS} | Total={total_rows} (shard {SHARD_INDEX}/{SHARD_STEP})")

    print(f"\n🚀 [{current_idx}/{total_rows}] FLAG: START")
    print(f"    Symbol : {symbol}")
    print(f"    URL    : {day_url}")
    print(f"    Date   : {target_date}  (from {DATE_SPREADSHEET_NAME}/{DATE_TAB_NAME} col {DATE_DATE_COL_LETTER})")

    driver = ensure_thread_driver_logged_in()

    try:
        driver.get(day_url)

        chart = wait_chart_ready(driver, timeout=18)
        force_clear_ads(driver)

        goto_date_fast(driver, chart, target_date)

        chart = wait_chart_ready(driver, timeout=12)
        force_clear_ads(driver)

        img = chart.screenshot_as_png

        month_val = "Unknown"
        try:
            month_val = datetime.strptime(target_date, "%Y-%m-%d").strftime('%B')
        except:
            pass

        ok = save_to_mysql(symbol, "day", img, target_date, month_val)
        if ok:
            print(f"✅ [{current_idx}/{total_rows}] FLAG: DB INSERT/UPDATE OK -> table=next_bagger_review_screenshot | {symbol} | day | {target_date}")
        else:
            print(f"❌ [{current_idx}/{total_rows}] FLAG: DB ERROR -> {symbol}")

    except Exception as e:
        print(f"⚠️ [{current_idx}/{total_rows}] FLAG: ERROR -> {symbol} -> {str(e)[:140]}")


# ---------------- MAIN ---------------- #
def main():
    global total_rows

    if not init_db_pool():
        return

    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)

        # ✅ load date mapping first
        load_date_map(gc)

        # Load main sheet (symbol + url)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()

        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        rows = df.to_dict("records")

        # ✅ shard rows (optional)
        if SHARD_STEP > 1:
            rows = [r for i, r in enumerate(rows) if (i % SHARD_STEP) == SHARD_INDEX]

        total_rows = len(rows)
        print(f"✅ FLAG: LOADED {total_rows} ROWS from '{SPREADSHEET_NAME}' / '{TAB_NAME}' (after shard)")
    except Exception as e:
        print(f"❌ FLAG: GOOGLE SHEETS ERROR: {e}")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, rows))

    with drivers_lock:
        for d in all_drivers:
            try:
                d.quit()
            except:
                pass

    print("\n🏁 FLAG: COMPLETED.")


if __name__ == "__main__":
    main()
