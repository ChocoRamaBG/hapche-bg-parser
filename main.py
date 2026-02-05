import time
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- 📁 ПЪТ КЪМ ПАПКИТЕ (PATHCHOVTSI) ---
# В облака (GitHub Actions) пишем в текущата директория
output_dir = "scraped_data"

if not os.path.exists(output_dir):
    try:
        os.makedirs(output_dir)
        print("📁 Папката не съществуваше, ама аз съм sigma male и ти я създадох.")
    except Exception as e:
        print(f"⚠️ ГРЕДА! Не мога да създам папката. Linux се прави на интересен: {e}")

output_filename = os.path.join(output_dir, "hapche_PRO_GRIND_SAVE.xlsx")
print(f"🎯 Файлът ще се казва: {output_filename}")

# --- ⚙️ НАСТРОЙКИ НА БРАУЗЪРА ЗА ОБЛАКА ---
options = Options()
# ТОВА Е ВАЖНО, ЛЬОЛЬО! Без това в GitHub Actions нищо няма да стане.
options.add_argument('--headless=new')  # Без графичен интерфейс, като душата ми
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--log-level=3')
# Малко fake user-agent, да не ни хванат веднага, че сме ботчовци
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# --- 🚗 СТАРТИРАНЕ НА ДРАЙВЪРЧОВЦИ ---
print("⏳ Паля гумите на Chrome в облака... Skibidi dop dop!")
try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("✅ Драйвърът зареди. Rizz level: 1000.")
except Exception as e:
    print(f"💥 Мамка му човече, драйвърът гръмна: {e}")
    # Пробваме пак без сървис мениджъра, ако гръмне (малини и къпини, все тая)
    driver = webdriver.Chrome(options=options)

# --- 💾 ЗАПИСВАЧКАТА ---
def save_single_record(record):
    if not record: return
    try:
        new_df = pd.DataFrame([record])
        if os.path.exists(output_filename):
            try:
                existing_df = pd.read_excel(output_filename)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            except:
                time.sleep(1)
                final_df = new_df 
        else:
            final_df = new_df

        final_df.to_excel(output_filename, index=False)
        print(f"💾 Докторът '{record.get('Име')}' е записан. Stonks 📈.")
    except Exception as e:
        print(f"❌ ERROR при запис: {e}. Данните изчезнаха в shadow realm-a.")

# --- 🕵️‍♂️ AGENT 007 ---
def scrape_details_from_profile(url, basic_info):
    print(f"   👉 Visiting: {url}")
    try:
        driver.get(url)
        # Brainrot wait time
        time.sleep(1.5) 

        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # --- HERO SECTION ---
        try:
            full_name = driver.find_element(By.XPATH, "//h1[@itemprop='name']").text.strip()
            basic_info["Име"] = full_name
        except: pass

        try:
            specialties_full = driver.find_element(By.CSS_SELECTOR, ".subtitle--category").text.strip()
            basic_info["Специалност (Профил)"] = specialties_full
        except: pass

        # --- STATISTICS ---
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

        # --- КОНТАКТИ ---
        phones = []
        try:
            phone_container = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Телефон')]/following-sibling::div[contains(@class, 'value')]")
            phone_divs = phone_container.find_elements(By.TAG_NAME, "div")
            if phone_divs:
                phones = [p.text.strip() for p in phone_divs if p.text.strip()]
            else:
                phones = [phone_container.text.strip()]
        except: pass
        
        phone_str = ", ".join(phones) if phones else "-"

        address_profile = "-"
        try:
            address_profile = driver.find_element(By.ID, "address-value").text.strip().replace('\n', ', ')
        except:
            try:
                address_profile = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Адрес')]/following-sibling::div[contains(@class, 'value')]").text.strip()
            except: pass

        work_time = "-"
        try:
            work_time = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Работно време')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: pass

        email = "-"
        try:
            email = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Електронна поща')]/following-sibling::div[contains(@class, 'value')]").text.strip()
        except: pass

        website = "-"
        try:
            website = driver.find_element(By.XPATH, "//div[contains(@class, 'label') and contains(text(), 'Интернет страница')]/following-sibling::div[contains(@class, 'value')]//a").get_attribute("href")
        except: pass

        basic_info.update({
            "Адрес (Профил)": address_profile,
            "Телефони": phone_str,
            "Работно време": work_time,
            "Email": email,
            "Website": website,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        return basic_info

    except Exception as e:
        print(f"💀 Грешка в профила (андибул морков ситуация): {e}")
        return basic_info

# --- 📜 MAIN LOOP (THE GRIND) ---
page = 1
# Няма max_pages, шефе. Until the wheels fall off.
print("🚀 Стартирам машината. Fanum tax on the data.")

try:
    while True:
        target_url = f"https://www.rating.hapche.bg/search/lekari-spetsialisti/-/-&page={page}"
        print(f"\n📄 --- СТРАНИЦА {page} ---")
        driver.get(target_url)
        
        try:
            # Чакаме таблицата или съобщение, че няма нищо
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.mr-table")))
            except:
                print("⛔ Няма таблица. Май стигнахме края или ни баннаха като нубове.")
                break

            rows = driver.find_elements(By.CSS_SELECTOR, "table.mr-table tbody tr")
            
            if not rows:
                print("⛔ Край на мача. Няма повече докторчовци.")
                break

            print(f"🔎 Намерих {len(rows)} профилчовци за обработка.")
            
            doctors_on_page = []
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, "td.name a")
                    name = name_el.text.strip()
                    url = name_el.get_attribute("href")
                    
                    city = "-"
                    try:
                        details = row.find_element(By.CSS_SELECTOR, "td.name span").text
                        if "гр." in details:
                            city = details.split("гр.")[1].split(",")[0].strip()
                            city = "гр. " + city
                    except: pass

                    doc_data = {
                        "Име": name,
                        "URL": url,
                        "Град (Таблица)": city
                    }
                    doctors_on_page.append(doc_data)
                except: continue

            # Влизаме във всеки (Grindset mode activated)
            for doc in doctors_on_page:
                if "search" in doc['URL']: continue
                full_data = scrape_details_from_profile(doc['URL'], doc)
                save_single_record(full_data)

            page += 1
            
        except Exception as e:
            print(f"🤬 ГРЕШКА на страница {page}: {e}")
            # Ако гръмне генерално, по-добре да спрем да не зациклим
            break

finally:
    try:
        driver.quit()
        print("🛑 Спрях колата.")
    except: pass
    print(f"\n🏁 Финито! Всичко е в папката '{output_dir}'. Bye bye, mogger.")
