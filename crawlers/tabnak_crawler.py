import requests
import re
from bs4 import BeautifulSoup
import csv
import sys
from datetime import datetime
import os
import time
import locale 

# ---- تنظیمات و مسیرها ----
SERVER_URL = "https://www.tabnak.ir/fa/news/"
PATH_LOG = "./log/tabnak_id.log"
OUTPUT_CSV = "Tabnak_ID_Dataset.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
# **اندازه بچ برای نوشتن روی دیسک**
BATCH_SIZE = 10 

# اطمینان از وجود پوشه log
os.makedirs("log", exist_ok=True)

# **تابع کمکی برای حذف کاراکترهای نامرئی جهت دهی**
def clean_rtl_chars(text):
    """کاراکترهای Left-to-Right Mark (U+200E) و Right-to-Left Mark (U+200F) را حذف می کند."""
    # U+200E: LRM, U+200F: RLM
    return text.replace('\u200e', '').replace('\u200f', '')

def initialize_crawl_range():
    """شروع و پایان ID را بر اساس ورودی خط فرمان یا فایل لاگ تعیین می کند."""
    if len(sys.argv) == 3:
        # ورودی از خط فرمان: python crawler.py 12345 12355
        start_id = sys.argv[1]
        end_id = sys.argv[2]
    else:
        # خواندن از فایل لاگ برای ادامه کراول
        try:
            with open(PATH_LOG, "r") as f:
                content = f.read().strip()
                if content:
                    start_id, end_id = content.split(",")
                else:
                    # اگر لاگ خالی است، یک رنج پیش فرض بزرگ را تعریف کنید
                    print("Log file is empty. Using default starting range (1000000 to 1000100).")
                    start_id = "1000000"
                    end_id = "1000100"
        except FileNotFoundError:
            # اگر لاگ وجود ندارد، رنج پیش فرض را استفاده کنید
            print("Log file not found. Using default starting range (1000000 to 1000100).")
            start_id = "1000000"
            end_id = "1000100"

    return int(start_id), int(end_id)

def ensure_csv_header():
    """اطمینان حاصل می کند که فایل CSV وجود دارد و هدر آن نوشته شده است."""
    
    # **بررسی می کند که آیا فایل وجود ندارد یا وجود دارد اما خالی است (اندازه 0).**
    write_header = not os.path.exists(OUTPUT_CSV) or (os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) == 0)

    if write_header:
        with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
            # **CSV Writer را با Quote All تنظیم می کنیم تا از مشکل نقل قول گذاری متن جلوگیری شود.**
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["title", "abstract", "body", "date_georgian_iso", "link"])

def write_batch_and_update_log(data_buffer, last_successful_id, end_id):
    """داده های جمع آوری شده را در CSV می نویسد و فایل لاگ را به روز می کند."""
    if not data_buffer:
        return

    try:
        # 1. نوشتن داده ها در CSV
        with open(OUTPUT_CSV, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerows(data_buffer)

        # 2. به روزرسانی لاگ (فقط در صورت موفقیت آمیز بودن نوشتن)
        # لاگ را به ID بعدی که باید شروع شود، تنظیم می کنیم.
        with open(PATH_LOG, "w") as f:
            f.write(f"{last_successful_id + 1},{end_id}")
        
        print(f"--- Batch written successfully. Resuming from ID {last_successful_id + 1} ---")
        
        # 3. پاکسازی بافر
        data_buffer.clear()

    except Exception as e:
        # در صورت بروز خطا در نوشتن، بافر پاک نمی شود و اسکریپت تلاش می کند از ID بعدی ادامه دهد.
        print(f"FATAL WRITE ERROR: Could not write batch to disk. Error: {e}")


def crawl():
    """حلقه اصلی کراولر را اجرا می کند."""
    start_id, end_id = initialize_crawl_range()
    ensure_csv_header()
    
    headers = {'User-Agent': USER_AGENT}
    data_buffer = [] # **بافر برای جمع آوری داده ها**

    for current_id in range(start_id, end_id):
        link = SERVER_URL + str(current_id)
        print(f"[{current_id}/{end_id}] Fetching: {link}")

        try:
            response = requests.get(link, timeout=15, headers=headers)
            
            # اگر 404 یا کد دیگری بود، ادامه بده
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            # ---- استخراج داده ها و تمیزسازی (Cleanup) ----
            
            # 1. عنوان (تیتر)
            title_tag = soup.select_one('h1.Htag, h1.title') 
            if not title_tag: continue
            title = clean_rtl_chars(title_tag.get_text(strip=True)) # **حذف کاراکترهای نامرئی**

            # 2. خلاصه (لید)
            subtitle_tag = soup.select_one('div.subtitle, div.lead')
            subtitle = clean_rtl_chars(subtitle_tag.get_text(strip=True)) if subtitle_tag else "" # **حذف کاراکترهای نامرئی**

            # 3. متن اصلی (بدنه)
            body_tag = soup.select_one('div.body, div.body div.rte')
            body_parts = []
            if body_tag:
                for tag in body_tag.find_all(['p', 'div', 'li']):
                    text = tag.get_text(strip=True)
                    if text:
                        body_parts.append(text)
            
            body = '\n'.join(body_parts) if body_parts else body_tag.get_text(strip=True) if body_tag else ""
            body = clean_rtl_chars(body) # **حذف کاراکترهای نامرئی از متن نهایی**
            if not body: continue

            # 4. تاریخ میلادی (Gregorian) - تمرکز بر روی en_date
            date_en_tag = soup.select_one('span.en_date') 
            date_iso = ""

            if date_en_tag:
                raw_date = date_en_tag.get_text(strip=True)
                
                try:
                    current_locale = locale.getlocale(locale.LC_TIME)
                    locale.setlocale(locale.LC_TIME, 'C') 
                    
                    cleaned_date = re.sub(r'\s+', ' ', raw_date).strip()

                    # فرمت: DD Month YYYY (مثلاً 02 September 2020)
                    date_obj = datetime.strptime(cleaned_date, "%d %B %Y")
                    date_iso = date_obj.date().isoformat() # تبدیل به فرمت استاندارد YYYY-MM-DD

                except ValueError:
                    print(f"Warning: Date format error for raw date: '{raw_date}'")
                    date_iso = raw_date
                finally:
                    locale.setlocale(locale.LC_TIME, current_locale)
            
            if not date_iso or date_iso == raw_date:
                continue

            # ---- ذخیره داده ها در بافر ----
            # توجه: متغیر link در ابتدای حلقه به عنوان آدرس کامل خبر تعریف شده است
            data_buffer.append([title, subtitle, body, date_iso, link])

            # **بررسی بچ برای نوشتن روی دیسک**
            if len(data_buffer) >= BATCH_SIZE:
                write_batch_and_update_log(data_buffer, current_id, end_id)

            # اعمال تأخیر برای جلوگیری از مسدود شدن IP (Fair Play)
            time.sleep(1) 

        except requests.exceptions.Timeout:
            print(f"Timeout occurred for ID {current_id}. Skipping.")
            time.sleep(5) 
            continue
        except Exception as e:
            print(f"An unexpected error occurred at ID {current_id}: {e}")
            time.sleep(3)
            continue
    
    # ---- نوشتن بچ نهایی پس از اتمام حلقه ----
    if data_buffer:
        print(f"Writing final batch of {len(data_buffer)} items.")
        # چون حلقه تمام شده است، ID لاگ را به end_id تنظیم می کنیم.
        write_batch_and_update_log(data_buffer, end_id - 1, end_id) 
        
if __name__ == "__main__":
    crawl()
