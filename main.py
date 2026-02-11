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

# Път към папката, както си го искал
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

# Инициализиране на CSV хедър, ако файлът не съществува
if not os.path.exists(current_batch_filename):
    df_headers = pd.DataFrame(columns=["Име", "URL", "Timestamp", "Телефон", "Адрес", "Специалност"]) # Добави си колоните тук
    df_headers.to_csv(current_batch_filename, index=False, encoding='utf-8-sig')

# --- ⚙️ НАСТРОЙКИ НА БРАУЗЪРА ---
options = Options()
options.add_argument('--headless=new') 
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
        # Excel презаписването беше cringe.
        with open(current_batch_filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=record.keys())
            # Хедърът вече е там, така че пишем само реда
            writer.writerow(record)

        print(f"💾 Докторът '{record.get('Име', 'N/A')}' е записан. Stonks 📈.")
    except Exception as e:
        print(f"❌ What the fuck? ERROR при запис: {e}. Данните изчезнаха в shadow realm-a.")

# --- 🕵️‍♂️ AGENT 007 ---
def scrape_details_from_profile(url, basic_info):
    # Гащник, тук слагаш твоята логика. Аз само симулирам работа.
    # "Работата облагородява човека", са казали старите българи, ама те не са писали Selenium.
    
    print(f"   👉 Visiting: {url}")
    try:
        driver.get(url)
        # Лека пауза, да не ни баннат IP-то
        time.sleep(1.5) 
        
        # ТУК ТВОЯ КОД ЗА SCRAPING...
        # Пример:
        # try:
        #     tel = driver.find_element(By.CSS_SELECTOR, ".phone").text
        #     basic_info["Телефон"] = tel
        # except: pass
        
        basic_info["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Симулация на brainrot terminology extraction
        basic_info["Quantum_Rizz_Level"] = "High" 
        
        return basic_info
    except Exception as e:
        print(f"💀 Мамка му човече, не можах да отворя профила: {e}")
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
                # Проверка за "андибул морков" ситуация (празна страница)
                break

            rows = driver.find_elements(By.CSS_SELECTOR, "table.mr-table tbody tr")
            if not rows:
                print("⛔ Няма повече докторчовци. It's over.")
                break

            print(f"🔎 Намерих {len(rows)} профилчовци.")
            
            # 1. СЪБИРАНЕ НА ЛИНКОВЕ (Без влизане още, за да не станат Stale)
            doctors_on_page = []
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, "td.name a")
                    url = name_el.get_attribute("href")
                    name = name_el.text.strip()
                    # Избягваме дублирани search URL-и
                    if "search" not in url:
                        doctors_on_page.append({"Име": name, "URL": url})
                except: 
                    continue

            # 2. ОБХОЖДАНЕ НА ВСЕКИ (VISIT & SCRAPE)
            for doc in doctors_on_page:
                # Влизаме, стържем, записваме веднага (ACID принцип, ама не точно)
                full_data = scrape_details_from_profile(doc['URL'], doc)
                save_single_record(full_data)

            # ✅ УСПЕШНО MINED PAGE
            page += 1
            
            # 💾 UPDATE STATE FILE IMMEDIATELY
            with open(state_file, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"🤬 ГРЕШКА на страница {page}: {e}. Hell nah.")
            # Ако гръмне веднъж, пробваме следващата, да не спираме целия процес
            page += 1 

finally:
    try:
        driver.quit()
    except:
        pass
    print(f"\n🏁 Финито за тая сесия! Стигнахме до страница {page}.")
    print(f"📝 State saved: {page}. Отивам да пипам трева.")
