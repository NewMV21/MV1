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

# ---------------- OPTIMIZED BROWSER INIT ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    # SPEED BOOST: Disable images and unnecessary CSS rendering
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.managed_default_content_settings.stylesheets": 2}
    opts.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json)) if creds_json else gspread.service_account(filename="credentials.json")
    source_sheet = client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = client.open_by_url(NEW_MV2_URL).worksheet("Sheet5")
    data_rows = source_sheet.get_all_values()[1:]
    print("✅ Sheets Connected")
except Exception as e:
    print(f"❌ Connection Error: {e}"); raise

current_date = date.today().strftime("%m/%d/%Y")
driver = get_driver()
batch, batch_start = [], None

# ---------------- SCRAPING LOOP ---------------- #
try:
    # Initial Cookie Load (Done once per run)
    if os.path.exists("cookies.json"):
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r") as f:
            for c in json.load(f):
                try: driver.add_cookie(c)
                except: pass
        driver.refresh()

    for i, row in enumerate(data_rows):
        if i < last_i or i < START_INDEX or i > END_INDEX:
            continue
        
        name = row[0]
        url = row[3] if len(row) > 3 else ""
        target_row = i + 2
        if batch_start is None: batch_start = target_row

        print(f"🔎 [{i+1}] Processing: {name}")

        try:
            driver.get(url)
            
            # YOUR EXACT XPATH & WAIT LOGIC
            WebDriverWait(driver, 25).until(
                EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
            )
            
            # YOUR EXACT BS4 & SELECTOR LOGIC
            soup = BeautifulSoup(driver.page_source, "html.parser")
            found_vals = [
                el.get_text().replace('−', '-').replace('∅', '').strip()
                for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
            ]
            
            # Pad to 14 columns as per your new requirement
            vals = found_vals[:14]
            while len(vals) < 14: vals.append("N/A")

        except Exception as e:
            print(f"  ⚠️ Skip {name}: {e}")
            vals = ["N/A"] * 14

        batch.append([name, current_date] + vals)

        # Batch Write (Optimized to 10 rows for API speed)
        if len(batch) >= 10:
            dest_sheet.update(f"A{batch_start}", batch)
            print(f"💾 Saved Rows {batch_start}-{target_row}")
            with open(CHECKPOINT_FILE, "w") as f: f.write(str(i + 1))
            batch, batch_start = [], None
            time.sleep(1)

finally:
    if batch:
        dest_sheet.update(f"A{batch_start}", batch)
    driver.quit()
    print("🏁 Process Finished.")
