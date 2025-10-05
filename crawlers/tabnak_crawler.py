import requests
import re
from bs4 import BeautifulSoup
import csv
import sys
from datetime import datetime
import os
import time
import locale # **اضافه شد: برای مدیریت زبان (Locale) در پردازش تاریخ میلادی**

# ---- تنظیمات و مسیرها ----
# آدرس پایه برای استخراج خبر بر اساس ID
SERVER_URL = "https://www.tabnak.ir/fa/news/"
PATH_LOG = "./log/tabnak_id.log"
OUTPUT_CSV = "Tabnak_ID_Dataset.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# اطمینان از وجود پوشه log
os.makedirs("log", exist_ok=True)

def initialize_crawl_range():
    """شروع و پایان ID را بر اساس ورودی خط فرمان یا فایل لاگ تعیین می‌کند."""
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
                    # اگر لاگ خالی است، یک رنج پیش‌فرض بزرگ را تعریف کنید
                    print("Log file is empty. Using default starting range (1000000 to 1000100).")
                    start_id = "1000000"
                    end_id = "1000100"
        except FileNotFoundError:
            # اگر لاگ وجود ندارد، رنج پیش‌فرض را استفاده کنید
            print("Log file not found. Using default starting range (1000000 to 1000100).")
            start_id = "1000000"
            end_id = "1000100"

    return int(start_id), int(end_id)

def ensure_csv_header():
    """اطمینان حاصل می‌کند که فایل CSV وجود دارد و هدر آن نوشته شده است."""
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline='', encoding="utf-8") as f:
            # **CSV Writer را با Quote All تنظیم می‌کنیم تا از مشکل نقل‌قول‌گذاری متن جلوگیری شود.**
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["title", "abstract", "body", "date_georgian_iso", "link"])

def crawl():
    """حلقه اصلی کراولر را اجرا می‌کند."""
    start_id, end_id = initialize_crawl_range()
    ensure_csv_header()
    
    headers = {'User-Agent': USER_AGENT}

    for current_id in range(start_id, end_id):
        link = SERVER_URL + str(current_id)
        print(f"[{current_id}/{end_id}] Fetching: {link}")

        try:
            response = requests.get(link, timeout=15, headers=headers)
            
            # اگر 404 یا کد دیگری بود، ادامه بده
            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, "html.parser")

            # ---- استخراج داده‌ها ----

            # 1. عنوان (تیتر)
            title_tag = soup.select_one('h1.Htag, h1.title') 
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            # 2. خلاصه (لید)
            subtitle_tag = soup.select_one('div.subtitle, div.lead')
            subtitle = subtitle_tag.get_text(strip=True) if subtitle_tag else ""

            # 3. متن اصلی (بدنه) - استفاده از روش قوی‌تر برای حفظ پاراگراف‌ها
            body_tag = soup.select_one('div.body, div.body div.rte')
            body_parts = []
            if body_tag:
                # به جای تنها get_text، پاراگراف‌ها را جدا می‌کنیم
                for tag in body_tag.find_all(['p', 'div', 'li']):
                    text = tag.get_text(strip=True)
                    if text:
                        body_parts.append(text)
            
            if not body_parts and body_tag: 
                body = body_tag.get_text(strip=True)
            else:
                body = '\n'.join(body_parts)

            if not body:
                continue

            # 4. تاریخ میلادی (Gregorian) - تمرکز بر روی en_date
            date_en_tag = soup.select_one('span.en_date') 
            date_iso = ""

            if date_en_tag:
                raw_date = date_en_tag.get_text(strip=True)
                
                # **تنظیم Locale به انگلیسی (C) برای پردازش صحیح نام ماه‌های انگلیسی**
                try:
                    # ذخیره Locale فعلی
                    current_locale = locale.getlocale(locale.LC_TIME)
                    # تنظیم Locale به انگلیسی (C/POSIX)
                    locale.setlocale(locale.LC_TIME, 'C') 
                    
                    # حذف هرگونه فضای اضافی داخلی (مثل دو فاصله بین کلمات)
                    cleaned_date = re.sub(r'\s+', ' ', raw_date).strip()

                    # فرمت: DD Month YYYY (مثلاً 02 September 2020)
                    date_obj = datetime.strptime(cleaned_date, "%d %B %Y")
                    date_iso = date_obj.date().isoformat() # تبدیل به فرمت استاندارد YYYY-MM-DD

                except ValueError:
                    print(f"Warning: Date format error for raw date: '{raw_date}'")
                    date_iso = raw_date
                finally:
                    # بازگرداندن Locale به حالت اولیه
                    locale.setlocale(locale.LC_TIME, current_locale)
            
            # اگر تاریخ میلادی پیدا نشد، این آیتم را رد می‌کنیم (چون هدف فاین تیونینگ است)
            if not date_iso or date_iso == raw_date:
                # اگر date_iso خالی است یا صرفاً رشته خام تاریخ است که تبدیل نشده، رد می‌شود.
                continue


            # ---- ذخیره داده‌ها ----
            with open(OUTPUT_CSV, "a", newline='', encoding="utf-8") as f:
                # **با تنظیم quoting=csv.QUOTE_ALL، تضمین می‌کنیم که فیلدهای حاوی کاما/خط جدید، نقل‌قول شوند.**
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow([title, subtitle, body, date_iso, link])

            # ---- به‌روزرسانی لاگ (فقط در صورت موفقیت) ----
            with open(PATH_LOG, "w") as f:
                f.write(f"{current_id + 1},{end_id}")
            
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

if __name__ == "__main__":
    crawl()
