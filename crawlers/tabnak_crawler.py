import requests
import time
import re
import csv
from bs4 import BeautifulSoup
from unidecode import unidecode
import sys
import os

# ---------------- Configuration ----------------
SERVER_URL = "https://www.tabnak.ir/fa/news/"
LOG_PATH = "./log/tabnak.log"
CSV_PATH = "./Tabnak_Dataset.csv"
BATCH_SIZE = 20
REQUEST_DELAY = 1.5  # seconds between requests

# ---------------- Range Setup ----------------
if len(sys.argv) > 2:
    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
else:
    if not os.path.exists(LOG_PATH):
        print("⚠️ Log file not found. Please create it manually with start,end values.")
        sys.exit(1)
    with open(LOG_PATH, "r") as f:
        start_id, end_id = map(int, f.read().strip().split(","))

# ---------------- Helper Functions ----------------
def update_log(current_id, end_id):
    """Update log file with latest progress."""
    with open(LOG_PATH, "w") as f:
        f.write(f"{current_id},{end_id}")

def clean_text(text):
    """Normalize text content."""
    return re.sub(r'\s+', ' ', text.strip())

def write_to_csv(file_path, rows, header_written):
    """Append a list of rows to CSV."""
    with open(file_path, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", "title", "abstract", "body",
            "date_shamsi", "time", "date_georgian",
            "view_count", "comment_count", "link"
        ])
        if not header_written:
            writer.writeheader()
        writer.writerows(rows)

# ---------------- Main Loop ----------------
batch_docs = []
header_written = os.path.exists(CSV_PATH)

for i in range(start_id, end_id):
    link = f"{SERVER_URL}{i}"
    print(f"Fetching: {link}")

    try:
        response = requests.get(link, timeout=10)
        if response.status_code != 200:
            print(f"Skipped {i} (status {response.status_code})")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # --- Extract fields ---
        title_tag = soup.select_one("h1.Htag")
        if not title_tag:
            continue
        title = clean_text(title_tag.get_text())

        subtitle_tag = soup.select_one("div.subtitle")
        subtitle = clean_text(subtitle_tag.get_text()) if subtitle_tag else ""

        body_tag = soup.select_one("div.body")
        body = clean_text(body_tag.get_text()) if body_tag else ""

        view_tag = soup.select_one("div.news_hits")
        view_count = int(re.findall(r'\d+', view_tag.get_text())[0]) if view_tag else 0

        comment_tag = soup.find("a", href="#comments")
        comments_count = unidecode(comment_tag.get_text().strip()) if comment_tag else "0"

        date_tag = soup.select_one("span.fa_date")
        time_tag = soup.select_one("span.en_date")
        if date_tag and time_tag and '-' in date_tag.get_text():
            date_shamsi, time_part = map(str.strip, date_tag.get_text().split('-'))
            date_georgian = time_tag.get_text().strip()
        else:
            date_shamsi, time_part, date_georgian = "", "", ""

        # --- Build document ---
        doc = {
            "id": i,
            "title": title,
            "abstract": subtitle,
            "body": body,
            "date_shamsi": date_shamsi,
            "time": time_part,
            "date_georgian": date_georgian,
            "view_count": view_count,
            "comment_count": comments_count,
            "link": link,
        }

        batch_docs.append(doc)

        # --- Save batch ---
        if len(batch_docs) >= BATCH_SIZE:
            write_to_csv(CSV_PATH, batch_docs, header_written)
            header_written = True
            update_log(i, end_id)
            print(f"💾 Saved {len(batch_docs)} rows up to ID {i}")
            batch_docs.clear()

        time.sleep(REQUEST_DELAY)

    except Exception as e:
        print(f"⚠️ Error at ID {i}: {e}")
        continue

# Save any remaining docs
if batch_docs:
    write_to_csv(CSV_PATH, batch_docs, header_written)
    update_log(end_id, end_id)
    print(f"💾 Saved final {len(batch_docs)} rows.")

print("✅ Crawling finished successfully.")
