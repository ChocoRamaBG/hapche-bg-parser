import time
import os
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

# --- ⚙️ КОНФИГУРАЦИЯ & BRAINROT ---
# Йо шефе, тук слагаме таймер, за да не ни убие GitHub като куче
START_TIME = time.time()
TIME_LIMIT_SECONDS = 5.5 * 60 * 60  # 5 часа и 30 минути (Fanum tax on time)

# Път към папката (folderchovtsi)
output_dir = "scraped_data"
state_file = "last_page.txt"  # Save point

# CSV файлът е по-добър от Excel за stream-ване на данни. 
# Excel е андибул морков технология.
current_batch_filename = os.path.join(output_dir, f"hapche_data.csv")

if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
        print("📁 Папката не съществуваше, ама аз съм sigma male и ти я създадох.")
    except Exception as e:
        print(f"⚠️ ГРЕДА! Не мога да създам папката. Linux се прави на интересен: {e}")

# --- 📜 ЧЕТЕНЕ НА STATE (SAVE GAME) ---
start_page = 1
if os.path.exists(state_file):
    try:
        with open(state_file, "r") as f:
            content = f.read().strip()
            if content.isdigit():
                start_page = int(content)
                print(f"🔄 Засичам Save Game! Продължаваме от страница {start_page}. W rizz.")
    except Exception:
        print("⚠️ Не можах да прочета state файла, почвам от 1. L bozo.")

# --- 📝 ДЕФИНИРАНЕ НА КОЛОНИТЕ (Fieldchovtsi) ---
# Тук добавихме всички нови полета от локалния скрипт, иначе CSV-то ще гръмне
fieldnames = [
    "Име", "URL", "Град (Таблица)", "Специалност (Профил)", 
    "Посещения (Профил)", "Рейтинг (Профил)", "Гласове (Профил)", 
    "Коментари (Профил)", "Адрес (Профил)", "Телефони", 
    "Работно време", "Email", "Website", "Timestamp"
]

# Инициализиране на CSV хедър, ако файлът не съществува
if not os.path.exists(current_batch_filename):
    try:
        with open(current_batch_filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        print("✅ CSV файлът е създаден с новите хедърчовци.")
    except Exception as e:
        print(f"❌ What the fuck? Не мога да създам CSV-то: {e}")

# --- ⚙️ НАСТРОЙКИ НА БРАУЗЪРА ---
options = Options()
# options.add_argument('--headless=new') # Пусни го headless, ако си на сървър, иначе го гледай как бачка
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--log-level=3')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# --- 🚗 СТАРТИРАНЕ НА ДРАЙВЪРЧОВЦИ ---
print("⏳ Паля гумите на Chrome... Skibidi dop dop!")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# --- 💾 ЗАПИСВАЧКАТА (CSV Edition) ---
def save_single_record(record):
    if not record: return
    try:
        # Използваме 'a' (append) режим. Това е O(1) операция. 
        # Тук използваме "non-blocking I/O injection" (пълна измислица, ама звучи яко)
        with open(current_batch_filename, 'a', newline='', encoding='utf-8-sig') as f:
            # extrasaction='ignore' е важно, за да не гърми ако имаме излишни ключове
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writerow(record)

        print(f"💾 Докторът '{record.get('Име', 'N/A')}' е записан. Stonks 📈.")
    except Exception as e:
        print(f"❌ What the fuck? ERROR при запис: {e}. Данните изчезнаха в shadow realm-a.")

# --- 🕵️‍♂️ AGENT 007: THE REAL DEAL (Взет от локалния код) ---
def scrape_details_from_profile(url, basic_info):
    """
    Това е истинската логика, а не онова менте от преди малко.
    """
    print(f"    👉 Visiting: {url}")
    try:
        driver.get(url)
        time.sleep(1.5) # Anti-ban cooldown (heuristic latency injection)

        # Чакаме body-то да се зареди, иначе сме чао
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # --- 1. HERO SECTION ---
        try:
            full_name = driver.find_element(By.XPATH, "//h1[@itemprop='name']").text.strip()
            basic_info["Име"] = full_name 
        except: pass

        try:
            specialties_full = driver.find_element(By.CSS_SELECTOR, ".subtitle--category").text.strip()
            basic_info["Специалност (Профил)"] = specialties_full
        except: pass

        # --- 2. STATISTICS ---
        stats_map = {
            "Посещения (Профил)": "visits-statistics-metadata-value",
            "Рейтинг (Профил)": "rating-statistics-metadata-value",
            "Гласове (Профил)": "votes-statistics-metadata-value",
            "Коментари (Профил)": "comments-statistics-metadata-value"
        }
        
        for key, div_id in stats_map.items():
            try:
                val = driver.find_element(By.ID, div_id).text.strip()
                basic_info[key] = val
            except: 
                basic_info[key] = "-"

        # --- 3. КОНТАКТИ ---
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

        # Адрес
        address_profile = "-"
        try:
            address_profile = driver.find_element(By.ID, "address-value").text.strip().replace('\n', ', ')
        except:
            try:
                address_profile = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Адрес')]/following-sibling::div[contains(@class, 'value')]").text.strip()
            except: pass
        basic_info["Адрес (Профил)"] = address_profile

        # Работно време
        try:
            basic_info["Работно време"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Работно време')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: basic_info["Работно време"] = "-"

        # Email & Web
        try:
            basic_info["Email"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Електронна поща')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: basic_info["Email"] = "-"

        try:
            basic_info["Website"] = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Интернет страница')]/following-sibling::div[contains(@class, 'value')]//a").get_attribute("href")
        except: basic_info["Website"] = "-"

        basic_info["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return basic_info

    except Exception as e:
        print(f"💀 Грешка в профила (андибул морков ситуация): {e}")
        # Връщаме каквото имаме, малини и къпини, все тая
        return basic_info

# --- 📜 MAIN LOOP (THE GRIND) ---
page = start_page
print(f"🚀 Стартирам от страница {page}. Let him cook.")

try:
    while True:
        # 🛑 CHECK TIME LIMIT 🛑
        elapsed_time = time.time() - START_TIME
        if elapsed_time > TIME_LIMIT_SECONDS:
            print(f"\n⚠️ ВРЕМЕТО ИЗТЕЧЕ! Минаха {elapsed_time/3600:.2f} часа.")
            print("🛑 Спирам за днес, че GitHub ще ни бие шамари.")
            break

        target_url = f"https://www.rating.hapche.bg/search/lekari-spetsialisti/-/-&page={page}"
        print(f"\n📄 --- СТРАНИЦА {page} ---")
        
        try:
            driver.get(target_url)
            
            # Умни чакания за таблица с докторчовци
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.mr-table")))
            except:
                print("⛔ Няма таблица. Май стигнахме края или сайтът е deadass счупен.")
                break

            rows = driver.find_elements(By.CSS_SELECTOR, "table.mr-table tbody tr")
            if not rows:
                print("⛔ Няма повече докторчовци. It's over.")
                break

            print(f"🔎 Намерих {len(rows)} профилчовци.")
            
            # 1. СЪБИРАНЕ НА ЛИНКОВЕ (Без влизане още)
            doctors_on_page = []
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, "td.name a")
                    url = name_el.get_attribute("href")
                    name = name_el.text.strip()
                    
                    # Град от таблицата
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
                except: 
                    continue

            # 2. ОБХОЖДАНЕ НА ВСЕКИ (VISIT & SCRAPE)
            for doc in doctors_on_page:
                full_data = scrape_details_from_profile(doc['URL'], doc)
                save_single_record(full_data)

            # ✅ УСПЕШНО MINED PAGE
            page += 1
            
            # 💾 UPDATE STATE FILE IMMEDIATELY
            with open(state_file, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"🤬 ГРЕШКА на страница {page}: {e}. Hell nah.")
            page += 1 

finally:
    try:
        driver.quit()
    except: pass
    print(f"\n🏁 Финито за тая сесия! Стигнахме до страница {page}.")
    print(f"📝 State saved: {page}. Отивам да пипам трева.")
