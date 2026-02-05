import time
import os
import pandas as pd
from datetime import datetime, timedelta
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
TIME_LIMIT_SECONDS = 5.5 * 60 * 60  # 5 часа и 30 минути (оставяме време за commit)

# Път към папката, както си го искал
output_dir = "scraped_data"
state_file = "last_page.txt"  # Тук ще пазим прогреса

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

# Файлът ще се казва динамично, за да не презаписваме старите данни
# Пример: hapche_batch_page_100_to_???.xlsx
current_batch_filename = os.path.join(output_dir, f"hapche_batch_start_{start_page}.xlsx")
print(f"🎯 Файлът за тази сесия ще се казва: {current_batch_filename}")

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

# --- 💾 ЗАПИСВАЧКАТА (Оптимизирана) ---
# Записваме в локален list и дъмпваме на всеки N човека или накрая, 
# но за сигурност при crash - append-ваме веднага.
def save_single_record(record):
    if not record: return
    try:
        new_df = pd.DataFrame([record])
        if os.path.exists(current_batch_filename):
            try:
                # Append mode за Excel е pain, но това работи
                with pd.ExcelWriter(current_batch_filename, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                     # Трябва да намерим последния ред, малко е хамалогия, 
                     # затова по-просто: четем всичко и презаписваме. 
                     # Бавно е, но е сигурно ("бавни" са и твоите рефлекси, Льольо).
                    existing_df = pd.read_excel(current_batch_filename)
                    final_df = pd.concat([existing_df, new_df], ignore_index=True)
                    final_df.to_excel(current_batch_filename, index=False)
            except:
                # Fallback
                new_df.to_excel(current_batch_filename, index=False)
        else:
            new_df.to_excel(current_batch_filename, index=False)

        print(f"💾 Докторът '{record.get('Име')}' е записан. Stonks 📈.")
    except Exception as e:
        print(f"❌ ERROR при запис: {e}. Данните изчезнаха в shadow realm-a.")

# --- 🕵️‍♂️ AGENT 007 ---
def scrape_details_from_profile(url, basic_info):
    # (Тук кодът е същият като твоя, спестявам място, но си го ползвай целия)
    # ... [COPY-PASTE твоята функция scrape_details_from_profile тук] ...
    # Само ще сложа dummy return за демото, ти си ползвай твоята логика!
    
    # ВНИМАНИЕ: Слагам минимална версия тук, за да не гърми скрипта ми,
    # ти си върни твоята пълна функция!
    print(f"   👉 Visiting: {url}")
    try:
        driver.get(url)
        # Brainrot delay
        time.sleep(1) 
        basic_info["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return basic_info
    except:
        return basic_info

# --- 📜 MAIN LOOP (THE GRIND) ---
page = start_page
print(f"🚀 Стартирам от страница {page}. Fanum tax on the data.")

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
            # Умни чакания...
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.mr-table")))
            except:
                print("⛔ Няма таблица. Май стигнахме края.")
                # Ако няма таблица, може би сме приключили завинаги?
                # Или просто е бъг. Нека запишем state += 1 за всеки случай.
                break

            rows = driver.find_elements(By.CSS_SELECTOR, "table.mr-table tbody tr")
            if not rows:
                print("⛔ Няма повече докторчовци.")
                break

            print(f"🔎 Намерих {len(rows)} профилчовци.")
            
            doctors_on_page = []
            # ... (Твоят код за събиране на линкове) ...
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, "td.name a")
                    url = name_el.get_attribute("href")
                    name = name_el.text.strip()
                    doctors_on_page.append({"Име": name, "URL": url})
                except: continue

            # Влизаме във всеки
            for doc in doctors_on_page:
                if "search" in doc['URL']: continue
                full_data = scrape_details_from_profile(doc['URL'], doc)
                save_single_record(full_data)

            # ✅ УСПЕШНО MINED PAGE
            page += 1
            
            # 💾 UPDATE STATE FILE IMMEDIATELY
            # Записваме след всяка страница, за да сме safe
            with open(state_file, "w") as f:
                f.write(str(page))

        except Exception as e:
            print(f"🤬 ГРЕШКА на страница {page}: {e}")
            break

finally:
    driver.quit()
    print(f"\n🏁 Финито за тая сесия! Стигнахме до страница {page}.")
    # Уверяваме се, че последната страница е записана
    with open(state_file, "w") as f:
        f.write(str(page))
    print(f"📝 State saved: {page}")
