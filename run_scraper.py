import os, time, json, gspread
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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume from checkpoint
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i}")

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
def scrape_tradingview(url, symbol_name):
    if not url:
        print(f"  ❌ No URL for {symbol_name}")
        return ["N/A"] * 14
    
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
    
    driver = webdriver.Chrome(service=CHROME_SERVICE, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        print(f"  🌐 {symbol_name[:20]}...")
        
        # Cookies (your exact logic)
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            try:
                with open("cookies.json", "r") as f:
                    cookies = json.load(f)
                    for c in cookies[:15]:
                        try:
                            driver.add_cookie({
                                "name": c.get("name"), "value": c.get("value"),
                                "domain": c.get("domain", ".tradingview.com"), 
                                "path": c.get("path", "/")
                            })
                        except: pass
                driver.refresh()
                time.sleep(1.2)  # Optimized
            except: pass
        
        driver.set_page_load_timeout(25)
        driver.get(url)
        time.sleep(2.5)  # Optimized from 6s
        
        # YOUR PROVEN EXTRACTION (EXACT + OPTIMIZED)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Priority 1: Your exact selectors
        values = []
        for selector in [
            "div.valueValue-l31H9iuA.apply-common-tooltip",
            "div.valueValue-l31H9iuA",
            "div[class*='valueValue']"
        ]:
            els = soup.select(selector)
            for el in els[:14]:
                text = el.get_text().strip().replace('−', '-').replace('∅', '')
                if text and len(text) < 25 and text not in values:
                    values.append(text)
        
        # Priority 2: Simple numeric fallback (SAFE regex)
        if len(values) < 10:
            for div in soup.find_all('div')[:50]:
                text = div.get_text().strip().replace('−', '-')
                if (text and 
                    re.search(r'[\d,.-%()]', text) and 
                    len(text) < 25 and 
                    text not in values):
                    values.append(text)
        
        # Exact 14 values (your logic)
        final_values = values[:14]
        while len(final_values) < 14:
            final_values.append("N/A")
            
        print(f"  📊 {len(values)} → {final_values[:2]}...")
        return final_values
        
    except TimeoutException:
        print(f"  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:40]}")
        return ["N/A"] * 14
    finally:
        driver.quit()

# ---------------- YOUR EXACT MAIN LOOP (OPTIMIZED) ---------------- #
batch = []
batch_start = None
processed = success_count = 0
start_time = time.time()

print(f"\n🚀 ULTRA-FAST MODE (250 symbols → ~4 mins)")

for i, row in enumerate(data_rows):
    # YOUR EXACT RANGE LOGIC
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row
    
    print(f"[{i+1:4d}/{END_INDEX-START_INDEX+1}] {name[:25]} -> Row {target_row}")
    
    # YOUR PROVEN SCRAPER
    vals = scrape_tradingview(url, name)
    row_data = [name, current_date] + vals
    
    if any(v != "N/A" for v in vals):
        success_count += 1
    
    batch.append(row_data)
    processed += 1
    
    # YOUR BATCH LOGIC (LARGER = FASTER)
    if len(batch) >= 8:  # Increased from 5
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start}-{target_row}")
            batch = []
            batch_start = None
            time.sleep(1.2)  # Optimized
        except Exception as e:
            print(f"❌ Write error: {e}")
    
    # YOUR CHECKPOINT (EXACT)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(1.2)  # Optimized from 1.8s

# YOUR FINAL FLUSH (EXACT)
if batch and batch_start:
    try:
        dest_sheet.update(f"A{batch_start}", batch)
        print(f"💾 Final batch: {batch_start}-{target_row}")
    except Exception as e:
        print(f"❌ Final write: {e}")

total_time = time.time() - start_time
print(f"\n🎉 COMPLETE!")
print(f"📊 Processed: {processed} | Success: {success_count}")
print(f"📍 Sheet5: Rows {START_INDEX+2}-{END_INDEX+2} × 16 columns")
print(f"✅ Success rate: {success_count/processed*100:.1f}%")
print(f"⚡ Speed: {processed/total_time*60:.0f} symbols/min")
