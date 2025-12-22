import os, time, json, gspread, re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint.txt")

# Resume Logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except: pass

# ---------------- HIGH-SPEED BROWSER SETUP ---------------- #
def get_optimized_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,800")
    
    # ⚡ SPEED OPTIMIZATION: Block Images and CSS
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.default_content_setting_values.notifications": 2
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver

# ---------------- DATA EXTRACTION ---------------- #
def extract_14_values(driver):
    try:
        # Wait for the main technical value container (max 10s)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )
        
        # Fast extraction using JS to avoid Python overhead
        vals = driver.execute_script("""
            return Array.from(document.querySelectorAll('.valueValue-l31H9iuA')).slice(0, 14).map(el => el.innerText);
        """)
        
        # Clean values
        cleaned = [v.replace('−', '-').replace('∅', '').strip() for v in vals if v]
        
        # Pad to 14
        while len(cleaned) < 14:
            cleaned.append("N/A")
        return cleaned[:14]
    except:
        return ["N/A"] * 14

# ---------------- MAIN EXECUTION ---------------- #
try:
    # Auth
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    
    current_date = date.today().strftime("%m/%d/%Y")
    driver = get_optimized_driver()
    batch, batch_start = [], None
    
    print(f"🚀 Starting Optimized Scrape: {last_i} to {END_INDEX}")

    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX:
            continue
            
        name = row[0]
        url = row[3] if len(row) > 3 else ""
        target_row = i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i+1}] {name}")
        
        driver.get(url)
        # Give JS a tiny moment to settle (TradingView is dynamic)
        time.sleep(1.5) 
        
        vals = extract_14_values(driver)
        batch.append([name, current_date] + vals)

        # Write every 10 rows (Efficient for Google Sheets API)
        if len(batch) >= 10:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Checkpoint Saved: Row {target_row}")
            with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
            batch, batch_start = [], None
            time.sleep(1)

finally:
    if batch:
        dest_sheet.update(f"A{batch_start}", batch)
    if 'driver' in locals():
        driver.quit()
    print("🏁 Process Complete.")
