import requests
import re
from bs4 import BeautifulSoup
from unidecode import unidecode
import csv
import sys
from datetime import datetime
import os

# ---- تنظیمات ----
server_url = "https://www.tabnak.ir/fa/news/"
path_log = "./log/tabnak.log"
output_csv = "Tabnak_Dataset.csv"

# اطمینان از وجود پوشه log
os.makedirs("log", exist_ok=True)

# ---- تعیین بازه شروع و پایان ----
if len(sys.argv) > 2:
    start = sys.argv[1]
    end = sys.argv[2]
else:
    with open(path_log, "r+") as f:
        start, end = str(f.read()).split(",")

# اگر فایل CSV وجود ندارد، هدر را بنویس
if not os.path.exists(output_csv):
    with open(output_csv, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "abstract", "body", "date_georgian", "link"])

# ---- حلقه اصلی ----
for i in range(int(start), int(end)):
    try:
        link = server_url + str(i)
        print(f"Fetching: {link}")
        response = requests.get(link, timeout=10)

        if response.status_code != 200:
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # عنوان
        title_tag = soup.select_one('h1.Htag')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)

        # خلاصه
        subtitle_tag = soup.select_one('div.subtitle')
        subtitle = subtitle_tag.get_text(strip=True) if subtitle_tag else ""

        # متن اصلی
        body_tag = soup.select_one('div.body')
        if not body_tag:
            continue
        body = body_tag.get_text(strip=True)

        # تاریخ میلادی
        date_tag = soup.select_one('span.en_date')
        if not date_tag:
            continue
        raw_date = date_tag.get_text(strip=True)

        # تبدیل تاریخ به فرمت استاندارد ISO
        try:
            date_obj = datetime.strptime(raw_date, "%Y/%m/%d")
            date_iso = date_obj.date().isoformat()
        except:
            date_iso = raw_date

        # ذخیره در CSV
        with open(output_csv, "a", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([title, subtitle, body, date_iso, link])

        # به‌روزرسانی لاگ
        with open(path_log, "w+") as f:
            f.write(f"{i},{end}")

    except Exception as e:
        print(f"Error at {i}: {e}")
        continue
