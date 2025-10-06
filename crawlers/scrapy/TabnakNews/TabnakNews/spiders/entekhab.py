import scrapy
import re

# توجه: jdatetime باید نصب شود تا تبدیل تاریخ شمسی به میلادی انجام شود.
import jdatetime
from datetime import datetime
from scrapy.item import Item, Field
import calendar


# -------------------------------------------------------------
# 1. تعریف ساختار داده (Item)
# -------------------------------------------------------------
class EntekhabItem(Item):
    """مدل داده خروجی برای ذخیره‌سازی در CSV."""

    title = Field()
    abstract = Field()
    body = Field()
    date_georgian_iso = Field()
    category = Field()
    link = Field()


# -------------------------------------------------------------
# 2. تعریف Spider
# -------------------------------------------------------------
class EntekhabSpider(scrapy.Spider):
    """
    Scrapy Spider برای کراول کردن Entekhab از طریق URL آرشیو پارامتری.
    """

    name = "entekhab_archive_crawler_parametric"
    allowed_domains = ["entekhab.ir"]

    # URL پایه آرشیو
    URL_BASE = "http://www.entekhab.ir/fa/archive?"

    # *** پارامترهای اصلی کراولینگ ***
    # 1. دسته‌بندی‌های هدف (IDهای تأیید شده توسط کاربر)
    CATEGORY_IDS = [
        # IDهای واقعی Entekhab
        {"id": 2, "name": "سیاسی"},
        {"id": 5, "name": "اقتصادی"},
        {"id": 9, "name": "ورزشی"},
        {"id": 8, "name": "فناوری"},
    ]

    # 2. بازه زمانی (شمسی)
    START_YEAR = 1400
    END_YEAR = 1404

    # *** تنظیمات پیش‌فرض تست (در صورت عدم تعیین از طریق خط فرمان) ***
    MAX_MONTH_TEST_DEFAULT = 1
    MAX_DAY_TEST_DEFAULT = 5
    MAX_PAGES_PER_DAY_DEFAULT = 1

    # 3. پارامترهای ثابت
    RPP = 50  # Rows Per Page (افزایش از 10 به 50 برای بهره‌وری بهتر)

    # تنظیمات داخلی و هوشمند Scrapy
    custom_settings = {
        "FEEDS": {
            "Entekhab_Parametric_Dataset.csv": {
                "format": "csv",
                "encoding": "utf8",
                "store_empty": False,
                "overwrite": False,
                "fields": [
                    "title",
                    "abstract",
                    "body",
                    "date_georgian_iso",
                    "category",
                    "link",
                ],
            }
        },
        "FEED_EXPORT_ENCODING": "utf8",
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 5.0,
        "ROBOTSTXT_OBEY": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 30,
    }

    def __init__(self, *args, **kwargs):
        super(EntekhabSpider, self).__init__(*args, **kwargs)
        # دریافت آرگومان‌های تست از خط فرمان
        self.max_month_test = int(
            kwargs.get("max_month_test", self.MAX_MONTH_TEST_DEFAULT)
        )
        self.max_day_test = int(kwargs.get("max_day_test", self.MAX_DAY_TEST_DEFAULT))
        self.max_pages_per_day = int(
            kwargs.get("max_pages_per_day", self.MAX_PAGES_PER_DAY_DEFAULT)
        )

        self.logger.info(
            f"CRAWL TEST SETTINGS: Months={self.max_month_test}, Days={self.max_day_test}, Pages/Day={self.max_pages_per_day}"
        )

    # -------------------------------------------------------------------
    # توابع کمکی
    # -------------------------------------------------------------------

    def clean_rtl_chars(self, text):
        """حذف کاراکترهای جهت‌دهی (LRE, PDF, LRM, RLM, etc.)."""
        text = text.replace("\u200e", "").replace("\u200f", "")
        text = text.replace("\u202a", "").replace("\u202c", "")
        text = text.replace("\u200c", " ")  # حذف نیم‌فاصله (Zero Width Non-Joiner)
        return text.strip()

    def convert_shamsi_to_georgian(self, shamsi_date_str):
        """تبدیل تاریخ شمسی (YYYY/MM/DD) به فرمت ISO میلادی."""
        try:
            parts = shamsi_date_str.strip().split("/")
            if len(parts) != 3:
                return None

            # تبدیل به jdate و سپس datetime
            jdate = jdatetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
            gdate = jdate.togregorian()
            return gdate.isoformat()
        except Exception as e:
            self.logger.error(f"Failed to convert date '{shamsi_date_str}': {e}")
            return None

    def start_requests(self):
        """ایجاد درخواست‌های اولیه با حلقه زدن روی سال، ماه و روز (Date-Major)."""

        for yr in range(self.START_YEAR, self.END_YEAR + 1):
            for mn in range(1, self.max_month_test + 1):
                # فرض می‌کنیم 6 ماه اول 31 روزه و بقیه 30 روزه هستند (برای سادگی تست)
                days_in_month = 31 if mn <= 6 else 30

                for dy in range(1, min(days_in_month, self.max_day_test) + 1):
                    for category in self.CATEGORY_IDS:
                        cat_id = category["id"]
                        cat_name = category["name"]

                        # ساخت URL آرشیو (p=1)
                        # استفاده از service_id=5 و sec_id=-1 از لینک جدید
                        url = (
                            f"{self.URL_BASE}service_id=5&sec_id=-1&cat_id={cat_id}&rpp={self.RPP}"
                            f"&from_date={yr}/{mn}/{dy}&to_date={yr}/{mn}/{dy}&p=1"
                        )

                        yield scrapy.Request(
                            url=url,
                            callback=self.parse_archive,
                            meta={
                                "category_name": cat_name,
                                "category_id": cat_id,
                                "page": 1,
                            },
                        )

    def parse_archive(self, response):
        """تجزیه و تحلیل صفحه آرشیو (استخراج لینک‌ها و Pagination)."""

        cat_name = response.meta["category_name"]
        current_page = response.meta["page"]

        # 1. استخراج لینک‌های خبر
        # انتخابگر بر اساس کد اصلی شما (a.title5)
        # توجه: این انتخابگر باید تأیید شود.
        news_links = response.css("div.archive_content a.title5::attr(href)").getall()

        if not news_links:
            self.logger.debug(
                f"No news links found for {cat_name} on page {current_page} - {response.url}. Stopping pagination for this day."
            )
            return

        for link in news_links:
            # لینک‌ها در انتخاب دارای پارامترهای اضافی هستند.
            # کراولر اصلی از regex برای حذف این پارامترها و نگه داشتن فقط ID استفاده می‌کرد.
            # ما مطمئن می‌شویم که لینک فقط شامل بخش عددی و URL پایه باشد.
            match = re.search(r"/\d+/", link)
            if match:
                clean_link = (
                    self.URL_BASE.split("/fa/archive?")[0] + link[: match.end()]
                )

                # ارسال لینک به تابع parse_news برای استخراج جزییات
                yield scrapy.Request(
                    clean_link,
                    callback=self.parse_news,
                    meta={"category_name": cat_name},
                )

        # 2. مدیریت Pagination (صفحه‌بندی)
        if current_page < self.max_pages_per_day:
            next_page = current_page + 1

            # ساخت URL صفحه بعدی (جایگزین کردن پارامتر p)
            new_url = re.sub(r"p=\d+", f"p={next_page}", response.url)

            # درخواست صفحه بعدی را ارسال می‌کنیم
            yield scrapy.Request(
                new_url,
                callback=self.parse_archive,
                meta={
                    "category_name": cat_name,
                    "category_id": response.meta["category_id"],
                    "page": next_page,
                },
            )

    def parse_news(self, response):
        """تابع اصلی تجزیه و تحلیل صفحه خبر."""

        item = EntekhabItem()
        item["category"] = response.meta.get("category_name", "نامشخص")

        # 1. استخراج ID به عنوان لینک (همانند کراولر تابناک)
        link_id = response.url.split("/")[
            -2
        ]  # ID در بخش قبل از اسلش نهایی قرار دارد (مثلاً 123456/)
        item["link"] = link_id

        # 2. استخراج تیتر
        title = response.css("h1::text").get()
        if not title:
            self.logger.debug(
                f"Skipping news item - Title not found for URL: {response.url}"
            )
            return
        item["title"] = self.clean_rtl_chars(title)

        # 3. استخراج خلاصه/لید
        abstract = response.css("div.subtitle::text").get() or ""
        item["abstract"] = self.clean_rtl_chars(abstract)

        # 4. استخراج بدنه خبر (بر اساس تأیید کاربر: div.khabar-matn)
        # از آنجایی که بدنه خبر حاوی کدهای جاوا اسکریپت بود، باید فقط متن را بدون تگ‌ها بگیریم.
        body_parts = response.css("div.khabar-matn *::text").getall()
        body = " ".join(p.strip() for p in body_parts if p.strip())

        # حذف کدهای جاوا اسکریپت که ممکن است در متن باقی مانده باشند (مانند var... error...)
        body = str(re.sub(r"(var.*?error.*}\);)", "", body))
        item["body"] = self.clean_rtl_chars(body)
        if not item["body"]:
            self.logger.debug(
                f"Skipping news item - Body not found for URL: {response.url}"
            )
            return

        # 5. استخراج تاریخ شمسی
        date_time_raw = response.css("div.news_pdate_c::text").get()
        date_iso = None
        if date_time_raw:
            # رشته خام: "تاریخ انتشار: ۰۹:۰۰ - ۱۸ مهر ۱۳۹۷"
            date_match = re.search(r"\d{2} \w+ \d{4}", date_time_raw)
            if date_match:
                # تبدیل از "dd month yyyy" شمسی به "yyyy/mm/dd"
                shamsi_date = date_match.group(0)

                # --- نگاشت ماه شمسی ---
                month_map = {
                    "فروردین": "01",
                    "اردیبهشت": "02",
                    "خرداد": "03",
                    "تیر": "04",
                    "مرداد": "05",
                    "شهریور": "06",
                    "مهر": "07",
                    "آبان": "08",
                    "آذر": "09",
                    "دی": "10",
                    "بهمن": "11",
                    "اسفند": "12",
                }

                # تبدیل اعداد فارسی به انگلیسی
                persian_to_english = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
                shamsi_date_str = date_match.group(0).translate(persian_to_english)

                parts = shamsi_date_str.split(" ")  # ['18', 'مهر', '1397']
                if len(parts) == 3 and parts[1] in month_map:
                    day = parts[0]
                    month = month_map[parts[1]]
                    year = parts[2]
                    date_for_conversion = f"{year}/{month}/{day}"
                    date_iso = self.convert_shamsi_to_georgian(date_for_conversion)

        item["date_georgian_iso"] = date_iso

        if not item["date_georgian_iso"]:
            self.logger.debug(
                f"Skipping news item - Date conversion failed for URL: {response.url}. Raw date: {date_time_raw}"
            )
            return

        yield item
