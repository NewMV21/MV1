import os, time, json, gspread, re, random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG (ALIGNED WITH YOUR SHEETS) ---------------- #
# Using your specific URLs and sheet names
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

# Sharding / Checkpoint logic
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- OPTIMIZED CHROME SETUP ---------------- #
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
    
    # SPEED OPTIMIZATION: Block images only (Keep CSS for TradingView data rendering)
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        gc = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        gc = gspread.service_account(filename="credentials.json")

    # Connect to your specific sheets and worksheets
    sheet_main = gc.open_by_url(STOCK_LIST_URL).worksheet('Sheet1')
    sheet_data = gc.open_by_url(NEW_MV2_URL).worksheet('Sheet5')
    
    # Batch read for speed
    all_rows = sheet_main.get_all_values()[1:] # Skip header
    name_list = [row[0] for row in all_rows]
    url_list = [row[3] if len(row) > 3 else "" for row in all_rows]
    current_date = date.today().strftime("%m/%d/%Y")
    print(f"✅ Connected. Processing symbols starting from index {last_i}")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    exit(1)

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        # Wait for your specific data container to appear
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )
        time.sleep(2) # Brief jitter to let JS finish numbers
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Your exact BeautifulSoup class and cleaning logic
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        # Standardize to 14 columns
        final_vals = values[:14]
        while len(final_vals) < 14:
            final_vals.append("N/A")
        return final_vals
        
    except Exception as e:
        print(f"  ⚠️ Error: {e}")
        return ["N/A"] * 14

# ---------------- MAIN LOOP ---------------- #
driver = get_driver()
buffer = []
BATCH_SIZE = 10 # Optimized batch for GitHub Actions performance

try:
    # Load Cookies (Persistent for the whole run)
    if os.path.exists("cookies.json"):
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r") as f:
            for c in json.load(f):
                try: driver.add_cookie(c)
                except: pass
        driver.refresh()

    for i in range(last_i, len(url_list)):
        # Sharding check
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        
        if i >= 2500: break # Hard stop for safety

        name = name_list[i]
        url = url_list[i]
        print(f"🔎 [{i}] {name}")

        row_data = [name, current_date] + scrape_tradingview(driver, url)
        buffer.append(row_data)

        # Write to Sheet5 in batches
        if len(buffer) >= BATCH_SIZE:
            sheet_data.append_rows(buffer, value_input_option='USER_ENTERED')
            print(f"💾 Saved {len(buffer)} rows. Index: {i}")
            buffer.clear()
            # Save Checkpoint
            with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        
        # Small jitter delay (Your proven logic)
        time.sleep(1.5 + random.random())

finally:
    if buffer:
        sheet_data.append_rows(buffer, value_input_option='USER_ENTERED')
    driver.quit()
    print("All done ✅")
