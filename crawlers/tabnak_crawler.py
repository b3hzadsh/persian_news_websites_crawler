import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from unidecode import unidecode
import sys
from datetime import datetime

# ---- تنظیمات ----
server_url = "https://www.tabnak.ir/fa/news/"
path_log = "./log/tabnak.log"

# ---- اتصال به MongoDB ----
mongo_server = "localhost"
mongo_port = 27017
client = MongoClient(mongo_server, mongo_port)
db = client['news_sites']
news = db['tabnak_clean']

# ---- تعیین بازه شروع و پایان ----
if len(sys.argv) > 2:
    start = sys.argv[1]
    end = sys.argv[2]
else:
    with open(path_log, "r+") as f:
        start, end = str(f.read()).split(",")

docs = []

# ---- حلقه اصلی ----
for i in range(int(start), int(end)):
    try:
        link = server_url + str(i)
        print(f"Fetching: {link}")
        response = requests.get(link, timeout=10)

        if response.status_code != 200:
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # استخراج عنوان
        title_tag = soup.select_one('h1.Htag')
        if not title_tag:
            continue
        title = title_tag.get_text(strip=True)

        # استخراج خلاصه
        subtitle_tag = soup.select_one('div.subtitle')
        subtitle = subtitle_tag.get_text(strip=True) if subtitle_tag else ""

        # استخراج متن اصلی خبر
        body_tag = soup.select_one('div.body')
        if not body_tag:
            continue
        body = body_tag.get_text(strip=True)

        # استخراج تاریخ میلادی
        date_tag = soup.select_one('span.en_date')
        if not date_tag:
            continue
        raw_date = date_tag.get_text(strip=True)

        # تمیز کردن و تبدیل تاریخ به فرمت ISO
        try:
            date_obj = datetime.strptime(raw_date, "%Y/%m/%d")
            date_iso = date_obj.date().isoformat()
        except:
            date_iso = raw_date  # اگر فرمت ناشناخته بود، همان متن خام ذخیره می‌شود

        # ساخت داکیومنت نهایی
        doc = {
            "title": title,
            "abstract": subtitle,
            "body": body,
            "date_georgian": date_iso,
            "link": link
        }

        docs.append(doc)

        # هر 20 خبر یک بار ذخیره در Mongo
        if len(docs) >= 20:
            news.insert_many(docs)
            docs.clear()

            with open(path_log, "w+") as f:
                f.write(f"{i},{end}")

    except Exception as e:
        print(f"Error at {i}: {e}")
        continue
