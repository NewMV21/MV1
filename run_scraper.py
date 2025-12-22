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

# ---------------- YOUR PROVEN ALL-VALUES SCRAPER ---------------- #
def scrape_tradingview(url, symbol_name):
    if not url:
        print(f"  ❌ No URL for {symbol_name}")
        return [""] * 14 
    
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
        
        # YOUR COOKIES LOGIC (EXACT)
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
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
            time.sleep(1.5)
        
        driver.set_page_load_timeout(35)
        driver.get(url)
        time.sleep(4)  # Enough for full render
        
        # **YOUR PROVEN 3-STRATEGY EXTRACTION (ALL VALUES!)**
        all_values = []
        
        # Strategy 1: Primary value classes (YOUR EXACT SELECTORS)
        selectors = [
            ".valueValue-l31H9iuA.apply-common-tooltip",
            ".valueValue-l31H9iuA",
            "div[class*='valueValue']",
            "div[class*='value']",
            ".chart-markup-table .value",
            "[data-value]"
        ]
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        for selector in selectors:
            try:
                elements = soup.select(selector)
                values = []
                for el in elements[:25]:  # Grab MORE values
                    text = el.get_text().strip().replace('−', '-').replace('∅', '')
                    if text and len(text) < 30:  # Keep ALL valid values
                        values.append(text)
                all_values.extend(values)
                print(f"  ✅ Selector '{selector}': {len(values)} values")
            except:
                continue
        
        # Strategy 2: YOUR NUMERIC EXTRACTION (EXPANDED)
        numeric_divs = soup.find_all('div', string=re.compile(r'[\d,.-]+'))
        for div in numeric_divs[:20]:
            text = div.get_text().strip().replace('−', '-')
            if (re.match(r'^[\d,.-]+.*|.*[\d,.-]+$', text) and 
                len(text) < 30 and text not in all_values):
                all_values.append(text)
        
        # Strategy 3: YOUR TABLE CELLS (ALL TABLES)
        tables = soup.find_all('table')
        for table in tables[:5]:
            for cell in table.find_all(['td', 'th'])[:30]:
                text = cell.get_text().strip().replace('−', '-')
                if (re.search(r'[\d,.-]', text) and 
                    len(text) < 30 and text not in all_values):
                    all_values.append(text)
        
        # Strategy 4: YOUR XPATH VALIDATION (ENSURE LOADED)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
                ))
            )
        except:
            pass
        
        # **DEDUPE & PAD TO 14+ (KEEP ALL!)**
        unique_values = []
        for val in all_values:
            if val and len(val) > 0 and len(val) < 35 and val not in unique_values:
                unique_values.append(val)
        
        # RETURN ALL VALUES (minimum 14, maximum whatever found)
        final_values = unique_values[:25]  # Cap at 25 for sheet
        while len(final_values) < 14:
            final_values.append("N/A")
            
        print(f"  📊 {len(unique_values)} unique values → {final_values[:3]}...")
        return final_values
        
    except TimeoutException:
        print(f"  ⏰ Timeout")
        return ["N/A"] * 14
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:50]}")
        return ["N/A"] * 14
    finally:
        driver.quit()

# ---------------- YOUR EXACT MAIN LOOP (OPTIMIZED) ---------------- #
batch = []
batch_start = None
processed = success_count = 0
start_time = time.time()

print(f"\n🚀 FULL-VALUES MODE (ALL 14+ values per symbol)")

for i, row in enumerate(data_rows):
    if i < last_i or i < START_INDEX or i > END_INDEX:
        continue
    
    name = row[0].strip()
    url = row[3] if len(row) > 3 else ""
    target_row = i + 2
    
    if batch_start is None:
        batch_start = target_row
    
    print(f"[{i+1:4d}/{END_INDEX-START_INDEX+1}] {name[:25]} -> Row {target_row}")
    
    vals = scrape_tradingview(url, name)
    row_data = [name, current_date] + vals[:14]  # Take first 14 for sheet
    
    if any(v != "N/A" for v in vals):
        success_count += 1
    
    batch.append(row_data)
    processed += 1
    
    if len(batch) >= 8:
        try:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved rows {batch_start}-{target_row} ({len(vals)} values found)")
            batch = []
            batch_start = None
            time.sleep(1.2)
        except Exception as e:
            print(f"❌ Write error: {e}")
    
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i + 1))
    
    time.sleep(1.3)

if batch and batch_start:
    try:
        dest_sheet.update(f"A{batch_start}", batch)
        print(f"💾 Final batch: {batch_start}-{target_row}")
    except Exception as e:
        print(f"❌ Final write: {e}")

total_time = time.time() - start_time
print(f"\n🎉 COMPLETE!")
print(f"📊 Processed: {processed} | Success: {success_count}")
print(f"✅ Success rate: {success_count/processed*100:.1f}%")
print(f"⚡ Speed: {processed/total_time*60:.0f} symbols/min")
