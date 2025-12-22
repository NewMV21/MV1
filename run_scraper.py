import os, time, json, gspread, re, random
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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except: pass

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Sheets Connected.")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

# ---------------- PERSISTENT BROWSER SETUP ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    # SPEED BOOST: Block Images
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def extract_values(driver):
    all_values = []
    # Strategy 1: CSS Selectors
    selectors = [".valueValue-l31H9iuA.apply-common-tooltip", ".valueValue-l31H9iuA"]
    for selector in selectors:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        for el in elements[:20]:
            val = el.text.strip().replace('−', '-').replace('∅', '')
            if val and len(val) < 25: all_values.append(val)
    
    # Strategy 2: Table Fallback
    if len(all_values) < 14:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for cell in soup.find_all(['td', 'div'], string=re.compile(r'[\d,.-]+'))[:20]:
            text = cell.get_text().strip().replace('−', '-')
            if text not in all_values and len(text) < 25: all_values.append(text)
    
    # Unique and Pad
    unique = []
    for v in all_values:
        if v not in unique: unique.append(v)
    
    final = unique[:14]
    while len(final) < 14: final.append("N/A")
    return final

# ---------------- MAIN LOOP ---------------- #
current_date = date.today().strftime("%m/%d/%Y")
driver = get_driver()
batch, batch_start = [], None
processed = success_count = 0

try:
    # Initial Cookie Load
    if os.path.exists("cookies.json"):
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r") as f:
            for c in json.load(f):
                try: driver.add_cookie(c)
                except: pass
        driver.refresh()

    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX: continue
        
        name, url = row[0].strip(), (row[3] if len(row) > 3 else "")
        target_row = i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i+1}] {name[:20]}")

        try:
            driver.get(url)
            # Smart wait for any value to appear
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
            time.sleep(1.5) # Brief jitter for numbers to settle
            
            vals = extract_values(driver)
            if any(v != "N/A" for v in vals): success_count += 1
            batch.append([name, current_date] + vals)
        except Exception as e:
            print(f"  ⚠️ Error: {e}")
            batch.append([name, current_date] + ["N/A"] * 14)

        processed += 1

        # Batch write every 10 rows for speed
        if len(batch) >= 10:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Rows {batch_start}-{target_row} saved.")
            with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
            batch, batch_start = [], None
            time.sleep(1)

finally:
    if batch: dest_sheet.update(f"A{batch_start}", batch)
    driver.quit()
    print(f"\n🏁 FINISHED. Success: {success_count}/{processed}")
