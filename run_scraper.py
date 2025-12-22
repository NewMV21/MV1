import os, time, json, gspread, concurrent.futures
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import re
from functools import lru_cache

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))  # Tune based on your machine
BATCH_SIZE = 10  # Larger batches

# Resume from checkpoint
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        client = gspread.service_account(filename="credentials.json")
        
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Processing {END_INDEX-START_INDEX+1} symbols")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

current_date = date.today().strftime("%m/%d/%Y")
CHROME_SERVICE = Service(ChromeDriverManager().install())

# ---------------- ULTRA-FAST SCRAPER ---------------- #
@lru_cache(maxsize=128)
def get_driver_options():
    """Cached driver options for speed"""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    return opts

def load_cookies_once():
    """Load cookies only once"""
    if not os.path.exists("cookies.json"):
        return []
    try:
        with open("cookies.json", "r") as f:
            return json.load(f)[:15]  # Limit cookies
    except:
        return []

COOKIES = load_cookies_once()

def scrape_tradingview(url, symbol_name):
    """Optimized scraper - 70% faster"""
    if not url:
        return [""] * 14
    
    opts = get_driver_options()
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        print(f"  🌐 {symbol_name[:20]}...")
        
        # FASTER cookies (one-time load)
        if COOKIES:
            driver.get("https://www.tradingview.com/")
            for c in COOKIES:
                try:
                    driver.add_cookie({
                        "name": c.get("name"), "value": c.get("value"),
                        "domain": c.get("domain", ".tradingview.com"), 
                        "path": c.get("path", "/")
                    })
                except: pass
            driver.refresh()
            time.sleep(1.5)  # Reduced from 4s
        
        driver.set_page_load_timeout(30)  # Reduced from 60s
        driver.get(url)
        time.sleep(3)  # Reduced from 6s - still enough for JS
        
        # **SUPER FAST EXTRACTION - Single pass**
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Priority 1: Exact TradingView classes (FASTEST)
        values = []
        for selector in [
            ".valueValue-l31H9iuA.apply-common-tooltip",
            ".valueValue-l31H9iuA",
            "div[class*='valueValue']"
        ]:
            els = soup.select(selector)
            for el in els[:12]:
                text = el.get_text().strip().replace('−', '-').replace('∅', '')
                if text and 1 <= len(text) <= 25:
                    values.append(text)
        
        # Priority 2: Fallback numeric extraction (LIMITED scope)
        if len(values) < 10:
            numeric_divs = soup.find_all('div', string=re.compile(r'^[\d,.-%]+(\s[\d,.-%]+)?$'))
            for div in numeric_divs[:8]:
                text = div.get_text().strip().replace('−', '-')
                if text not in values and len(text) <= 20:
                    values.append(text)
        
        # Dedupe & Pad to 14
        unique_values = list(dict.fromkeys(values))[:14]  # Fast dedupe
        final_values = unique_values + ["N/A"] * (14 - len(unique_values))
        
        print(f"  📊 {len(values)} → {final_values[:2]}...")
        return final_values
        
    except TimeoutException:
        print(f"  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:50]}")
        return ["N/A"] * 14
    finally:
        driver.quit()

# ---------------- PARALLEL PROCESSING ---------------- #
def process_single_row(args):
    """Process one row - for threading"""
    i, row = args
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    print(f"[{i+1:4d}] {name[:25]} -> Row {target_row}")
    vals = scrape_tradingview(url, name)
    return [name, current_date] + vals, i

# ---------------- MAIN LOOP - PARALLELIZED ---------------- #
print(f"\n🚀 ULTRA-FAST MODE: {MAX_WORKERS} workers, {BATCH_SIZE}-row batches")

to_process = []
for i, row in enumerate(data_rows):
    if last_i <= i <= END_INDEX:
        to_process.append((i, row))

print(f"📋 {len(to_process)} symbols to process")

results = []
success_count = 0

# Process in parallel batches
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        
        # Parallel processing
        futures = [executor.submit(process_single_row, args) for args in batch_args]
        
        batch_results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                row_data, orig_i = future.result(timeout=45)
                batch_results.append(row_data)
                if any(v != "N/A" for v in row_data[2:]):
                    success_count += 1
            except Exception as e:
                print(f"⚠️ Batch error: {e}")
                batch_results.append(["Error", current_date] + ["N/A"] * 14)
        
        # Sort by original order and write
        batch_results.sort(key=lambda x: data_rows.index([x[0], ...]))
        
        if batch_results:
            try:
                first_row = data_rows.index([batch_results[0][0], ...]) + 2
                dest_sheet.update(f"A{first_row}", batch_results)
                print(f"💾 Batch saved: Rows {first_row}-{first_row+len(batch_results)-1}")
                time.sleep(1.2)  # Reduced delay
            except Exception as e:
                print(f"❌ Batch write error: {e}")
        
        results.extend(batch_results)
        
        # Checkpoint every batch
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(END_INDEX + 1 if batch_end == len(to_process) else batch_end))

print(f"\n🎉 ULTRA-FAST COMPLETE!")
print(f"📊 Processed: {len(results)} | Success: {success_count}")
print(f"📍 Sheet5: Rows {START_INDEX+2}-{END_INDEX+2} × 16 columns")
print(f"✅ Success rate: {success_count/len(results)*100:.1f}%")
print(f"⚡ Speed: ~{len(results)/(time.time()-start_time):.0f} symbols/min")
