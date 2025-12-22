from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import random
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Batch read once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CUSTOM EXPECTED CONDITION ---------------- #
# This class waits until at least 'min_count' elements of the given class
# have non-empty text content, ensuring the data is fully rendered.
class text_content_loaded:
    """An expectation for checking that text content has loaded."""
    def __init__(self, locator, min_count=1):
        self.locator = locator
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(*self.locator)
        non_empty_count = 0
        if len(elements) > 0:
            for el in elements:
                if el.text.strip():
                    non_empty_count += 1
            # Return elements if enough have non-empty text, otherwise return False
            if non_empty_count >= self.min_count:
                return elements
        return False
        
# ---------------- SCRAPER ---------------- #
# --- UPDATED: Using Custom Expected Condition to wait for data content ---
def scrape_tradingview(driver, company_url):
    DATA_LOCATOR = (By.CLASS_NAME, "valueValue-l31H9iuA") 
    
    try:
        driver.get(company_url)
        
        # We wait up to 75 seconds for at least 10 data elements to be visible AND contain text.
        # Note: We use the shorter class name for robust element locating.
        WebDriverWait(driver, 75).until(
            text_content_loaded(DATA_LOCATOR, min_count=10) 
        )
        
        # After the wait succeeds, the page is fully loaded with content.
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Re-using your original BeautifulSoup logic to scrape the text
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            # The class below is for the data values that we want to extract
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        if not values:
            print(f"⚠️ Scraping successful but BeautifulSoup found no data for {company_url}.")
            
        return values
        
    except (NoSuchElementException, TimeoutException):
        print(f"⚠️ Scraping failed (Timeout or Element Not Found) for {company_url}.")
        return []
    except Exception as e:
        print(f"🚨 Unexpected error scraping {company_url}: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
# Initialize the driver once
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
except Exception as e:
    print(f"Error initializing WebDriver: {e}")
    exit(1)


# Load cookies (once per shard)
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r", encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        try:
            cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            cookie_to_add['secure'] = cookie.get('secure', False)
            cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
            if 'expiry' in cookie and cookie['expiry'] not in [None, '']:
                 cookie_to_add['expiry'] = int(cookie['expiry'])
            
            driver.add_cookie(cookie_to_add)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2)
else:
    print("⚠️ cookies.json not found, scraping without login. Login may be required for full data access.")

buffer = []
BATCH_SIZE = 50

# Start loop from the last successful checkpoint
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    
    # We aim for 2235, setting a generous hard stop at 2500
    if i > 2500: 
         print("Reached scraping limit of 2500. Stopping.")
         break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    if values:
        buffer.append([name, current_date] + values)
    else:
        print(f"Skipping {name}: no data")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Write every 50 rows
    if len(buffer) >= BATCH_SIZE:
        try:
            sheet_data.append_rows(buffer, table_range='A1') 
            print(f"✅ Wrote batch of {len(buffer)} rows. Current row index: {i}")
            buffer.clear()
        except Exception as e:
            print(f"⚠️ Batch write failed: {e}. Data remaining in buffer.")

    # Sleep with jitter (1.5s to 3.0s) for rate limit avoidance
    sleep_time = 1.5 + random.random() * 1.5 
    time.sleep(sleep_time)

# Final flush of any remaining items in the buffer
if buffer:
    try:
        sheet_data.append_rows(buffer, table_range='A1')
        print(f"✅ Final batch of {len(buffer)} rows written.")
    except Exception as e:
        print(f"⚠️ Final write failed: {e}")

driver.quit()
print("All done ✅")
