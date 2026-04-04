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
def log(msg): print(msg, flush=True)

def safe_str(e, n=260):
    try: return str(e).replace("\n", " ")[:n]
    except: return "error"

def col_letter_to_index(letter):
    n=0
    for ch in letter.upper():
        if "A"<=ch<="Z": n=n*26+(ord(ch)-64)
    return n-1

def normalize_date(val):
    if not val: return ""
    s=re.sub(r"[^\d/\-]","",str(val))
    for fmt in ("%Y-%m-%d","%d-%m-%Y","%d/%m/%Y","%Y/%m/%d"):
        try: return datetime.strptime(s,fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def read_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            return int(open(CHECKPOINT_FILE).read().strip())
    except: pass
    return -1

def write_checkpoint(i):
    try: open(CHECKPOINT_FILE,"w").write(str(i))
    except: pass

# ---------------- DATE MAP ---------------- #
def load_date_map(gc):
    global DATE_MAP
    ss = gc.open(DATE_SPREADSHEET_NAME)
    ws = ss.worksheet(DATE_TAB_NAME)
    vals = ws.get_all_values()

    si = col_letter_to_index(DATE_SYMBOL_COL)
    di = col_letter_to_index(DATE_COL_LETTER)

    for r in vals:
        if len(r)>max(si,di):
            sym=r[si].strip()
            dt=normalize_date(r[di])
            if sym and dt:
                DATE_MAP[sym.upper()] = dt

    log(f"✅ DATE MAP loaded: {len(DATE_MAP)}")

# ---------------- DB ---------------- #
def init_db_pool():
    global db_pool
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="pool",
        pool_size=max(2,MAX_THREADS),
        **DB_CONFIG
    )

def save_to_mysql(symbol,timeframe,img,date,month):
    try:
        conn=db_pool.get_connection()
        cur=conn.cursor()

        cur.execute(f"""
        INSERT INTO {TARGET_TABLE} (symbol,timeframe,screenshot,chart_date,month_before)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        screenshot=VALUES(screenshot),
        chart_date=VALUES(chart_date),
        month_before=VALUES(month_before),
        created_at=CURRENT_TIMESTAMP
        """,(symbol,timeframe,img,date,month))

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        log(f"❌ DB ERROR {symbol}: {safe_str(e)}")
        return False

# ---------------- BROWSER ---------------- #
def get_driver():
    opts=Options()
    opts.binary_location="/usr/bin/chromium"
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,900")
    return webdriver.Chrome(options=opts)

def ensure_driver():
    if not getattr(thread_local,"driver",None):
        d=get_driver()
        thread_local.driver=d
        d.get("https://www.tradingview.com/chart/")
    return thread_local.driver

def wait_chart(driver):
    WebDriverWait(driver,20).until(
        lambda d: d.execute_script("return document.readyState")=="complete")
    return WebDriverWait(driver,20).until(
        EC.presence_of_element_located((By.XPATH,"//div[contains(@class,'chart-container')]")))

def goto_date(driver,chart,date):
    ActionChains(driver).move_to_element(chart).click().perform()
    ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
    box=WebDriverWait(driver,10).until(
        EC.visibility_of_element_located((By.XPATH,"//input")))
    box.send_keys(Keys.CONTROL,"a")
    box.send_keys(date)
    box.send_keys(Keys.ENTER)
    time.sleep(1)

def stable(chart):
    last=None
    for _ in range(6):
        h=hashlib.md5(chart.screenshot_as_png).hexdigest()
        if h==last: return True
        last=h
        time.sleep(0.4)
    return True

def capture(driver,url,symbol,date):
    try:
        driver.get(url)
        chart=wait_chart(driver)
        goto_date(driver,chart,date)
        chart=wait_chart(driver)
        stable(chart)
        return chart.screenshot_as_png
    except Exception as e:
        log(f"⚠️ CAPTURE FAIL {symbol}: {safe_str(e)}")
        return None

# ---------------- WORKER ---------------- #
def process_row(task):
    global processed_count, db_ok, db_fail

    i,row=task
    row={k.lower():v for k,v in row.items()}

    symbol=row.get("symbol","").strip()
    day=row.get("day","").strip()
    week=row.get("week","").strip()

    if not symbol:
        write_checkpoint(i); return

    date=DATE_MAP.get(symbol.upper(),"")
    if not date:
        write_checkpoint(i); return

    driver=ensure_driver()

    day_img = capture(driver,day,symbol,date) if "tradingview" in day else None
    week_img = capture(driver,week,symbol,date) if "tradingview" in week else None

    month="Unknown"
    try: month=datetime.strptime(date,"%Y-%m-%d").strftime("%B")
    except: pass

    ok=True
    if day_img:
        if not save_to_mysql(symbol,"day",day_img,date,month): ok=False
    if week_img:
        if not save_to_mysql(symbol,"week",week_img,date,month): ok=False

    if ok: db_ok+=1
    else: db_fail+=1

    write_checkpoint(i)

# ---------------- MAIN ---------------- #
def main():
    global total_rows

    creds=json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc=gspread.service_account_from_dict(creds)

    load_date_map(gc)

    ws=gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    vals=ws.get_all_values()

    df=pd.DataFrame(vals[1:],columns=vals[0])
    rows=df.to_dict("records")

    last=read_checkpoint()
    rows=list(enumerate(rows))[last+1:]

    total_rows=len(rows)

    init_db_pool()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        ex.map(process_row, rows)

    log(f"✅ DONE: {db_ok} success | {db_fail} fail")

if __name__=="__main__":
    main()
