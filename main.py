import time
import os
import sys  # <--- ТРЯБВА НИ ЗА EXIT CODES
import pandas as pd
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

# --- ⚙️ КОНФИГУРАЦИЯ & BRAINROT ---
START_TIME = time.time()
# Намаляме малко времето, за да сме сигурни, че има време за commit
TIME_LIMIT_SECONDS = 5.4 * 60 * 60  # Оставяме малко buffer time, да не гръмне runner-а

output_dir = "scraped_data"
state_file = "last_page.txt" 
current_batch_filename = os.path.join(output_dir, f"hapche_data.csv")

if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
        print("📁 Папката е готова. Skibidi dop yes.")
    except Exception as e:
        print(f"⚠️ ГРЕДА! Не мога да създам папката: {e}")

# --- 📜 ЧЕТЕНЕ НА STATE ---
start_page = 1
if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                start_page = int(content)
                print(f"🔄 Loading save game... Level {start_page}.")
    except Exception:
        print("⚠️ Corrupted save file. Starting fresh.")

# --- 📝 ДЕФИНИРАНЕ НА КОЛОНИТЕ ---
fieldnames = [
    "Име", "URL", "Град (Таблица)", "Специалност (Профил)", 
    "Посещения (Профил)", "Рейтинг (Профил)", "Гласове (Профил)", 
    "Коментари (Профил)", "Адрес (Профил)", "Телефони", 
    "Работно време", "Email", "Website", "Timestamp"
]

if not os.path.exists(current_batch_filename):
    try:
        with open(current_batch_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception as e:
        print(f"❌ Error creating CSV: {e}")

# --- ⚙️ НАСТРОЙКИ НА БРАУЗЪРА ---
options = Options()
options.add_argument('--headless=new') 
options.add_argument('--no-sandbox')    
options.add_argument('--disable-dev-shm-usage') 
options.add_argument('--disable-gpu')   
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--log-level=3')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

print("⏳ Summoning Chrome Demon (Headless Mode)...")
try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("✅ Драйвърът захапа! Vamos a la playa!")
except Exception as e:
    print(f"💥 FATAL ERROR при стартиране на Chrome: {e}")
    sys.exit(1) # Exit with error

# --- 🍪 COOKIE MONSTER SLAYER ---
def nuke_cookie_popups(driver):
    # Търси и унищожава досадните бисквитчовци
    try:
        accept_btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.fc-cta-consent"))
        )
        driver.execute_script("arguments[0].click();", accept_btn)
    except: pass 
    try:
        accept_btn = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.cc-nb-okagree"))
        )
        driver.execute_script("arguments[0].click();", accept_btn)
    except: pass

# --- 💾 ЗАПИСВАЧКАТА ---
def save_single_record(record):
    if not record: return
    try:
        with open(current_batch_filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(record)
        print(f"💾 {record.get('Име', 'Unknown')} -> Saved. W.")
    except Exception as e:
        print(f"❌ Save error: {e}")

# --- 🕵️‍♂️ AGENT 007 ---
def scrape_details_from_profile(url, basic_info):
    print(f"    👉 Visiting: {url}")
    try:
        driver.get(url)
        nuke_cookie_popups(driver)
        
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except: pass

        # --- HERO SECTION ---
        try: basic_info["Име"] = driver.find_element(By.XPATH, "//h1[@itemprop='name']").text.strip()
        except: pass

        try: basic_info["Специалност (Профил)"] = driver.find_element(By.CSS_SELECTOR, ".subtitle--category").text.strip()
        except: pass

        # --- STATISTICS ---
        stats_map = {
            "Посещения (Профил)": "visits-statistics-metadata-value",
            "Рейтинг (Профил)": "rating-statistics-metadata-value",
            "Гласове (Профил)": "votes-statistics-metadata-value",
            "Коментари (Профил)": "comments-statistics-metadata-value"
        }
        for key, div_id in stats_map.items():
            try: basic_info[key] = driver.find_element(By.ID, div_id).text.strip()
            except: basic_info[key] = "-"

        # --- KONTAKTI ---
        phones = []
        try:
            phone_container = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Телефон')]/following-sibling::div[contains(@class, 'value')]")
            phone_divs = phone_container.find_elements(By.TAG_NAME, "div")
            if phone_divs:
                phones = [p.text.strip() for p in phone_divs if p.text.strip()]
            else:
                phones = [phone_container.text.strip()]
        except: pass
        basic_info["Телефони"] = ", ".join(phones) if phones else "-"

        # Adres
        try:
            basic_info["Адрес (Профил)"] = driver.find_element(By.ID, "address-value").text.strip().replace('\n', ', ')
        except:
            try:
                basic_info["Адрес (Профил)"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Адрес')]/following-sibling::div[contains(@class, 'value')]").text.strip()
            except: basic_info["Адрес (Профил)"] = "-"

        # Info
        try: basic_info["Работно време"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Работно време')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: basic_info["Работно време"] = "-"

        try: basic_info["Email"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Електронна поща')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: basic_info["Email"] = "-"
        
        try: basic_info["Website"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Интернет страница')]/following-sibling::div[contains(@class, 'value')]//a").get_attribute("href")
        except: basic_info["Website"] = "-"

        basic_info["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return basic_info

    except Exception as e:
        print(f"💀 Profile error (андибул морков): {e}")
        return basic_info

# --- 📜 MAIN LOOP ---
page = start_page
print(f"🚀 Starting grind from page {page}.")
exit_code = 0 # По подразбиране приемаме, че ще свършим нормално

try:
    while True:
        # 🛑 TIMEOUT CHECK
        if (time.time() - START_TIME) > TIME_LIMIT_SECONDS:
            print("🛑 Time limit reached. Suspending operation.")
            print("Ще се видим в следващия run, копеле.")
            exit_code = 2 # SPECIAL CODE: RESTART ME PLEASE
            break

        target_url = f"https://www.rating.hapche.bg/search/lekari-spetsialisti/-/-&page={page}"
        print(f"\n📄 --- PAGE {page} ---")
        
        try:
            driver.get(target_url)
            nuke_cookie_popups(driver)

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.mr-table")))
            except:
                print("⛔ No table found. End of the line.")
                exit_code = 0 # Success finish
                break

            rows = driver.find_elements(By.CSS_SELECTOR, "table.mr-table tbody tr")
            if not rows:
                print("⛔ No doctors found. Looks like we are done.")
                exit_code = 0
                break

            print(f"🔎 Found {len(rows)} potential victims (doctorchovci).")
            
            doctors_on_page = []
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, "td.name a")
                    url = name_el.get_attribute("href")
                    name = name_el.text.strip()
                    
                    city = "-"
                    try:
                        details = row.find_element(By.CSS_SELECTOR, "td.name span").text
                        if "гр." in details:
                            city = "гр. " + details.split("гр.")[1].split(",")[0].strip()
                    except: pass

                    if "search" not in url:
                        doctors_on_page.append({
                            "Име": name, 
                            "URL": url,
                            "Град (Таблица)": city
                        })
                except: continue

            # Scraping Loop
            for doc in doctors_on_page:
                full_data = scrape_details_from_profile(doc['URL'], doc)
                save_single_record(full_data)

            # Mark page as done
            page += 1
            with open(state_file, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"🤬 Page error: {e}")
            # Пробваме да минем напред, малини и къпини, все тая
            page += 1 

finally:
    try: driver.quit()
    except: pass
    print(f"\n🏁 Finished block. Last page: {page}. Exit Code: {exit_code}")
    sys.exit(exit_code)
