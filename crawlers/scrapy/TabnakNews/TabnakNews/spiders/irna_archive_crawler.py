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
class IrnaItem(Item):
    """مدل داده خروجی برای ذخیره‌سازی در CSV."""

    title = Field()
    abstract = Field()
    body = Field()
    date_georgian_iso = Field()
    category = Field()  # فیلد جدید برای ذخیره دسته‌بندی
    link = Field()


# -------------------------------------------------------------
# 2. تعریف Spider
# -------------------------------------------------------------
class IrnaSpider(scrapy.Spider):
    """
    Scrapy Spider برای کراول کردن IRNA از طریق URL آرشیو پارامتری (تاریخ محور) با استفاده از Playwright.
    """

    name = "irna_archive_crawler_parametric"
    allowed_domains = ["irna.ir"]

    # URL پایه آرشیو
    URL_BASE = "https://www.irna.ir/archive?"

    # *** پارامترهای اصلی کراولینگ ***
    # 1. دسته‌بندی‌های هدف (IDهای تأیید شده توسط کاربر)
    CATEGORY_IDS = [
        {"id": 20, "name": "اقتصادی"},
        {"id": 14, "name": "ورزشی"},
        {"id": 1, "name": "بین‌الملل"},
        {"id": 80, "name": "علم و فناوری"},
    ]

    # 2. بازه زمانی (شمسی)
    START_YEAR = 1404
    END_YEAR = 1404

    # *** تنظیمات پیش‌فرض تست (در صورت عدم تعیین از طریق خط فرمان) ***
    MAX_MONTH_TEST_DEFAULT = 10
    MAX_DAY_TEST_DEFAULT = 5
    MAX_PAGES_PER_DAY_DEFAULT = 1

    # 3. پارامترهای ثابت
    TP = 20  # فرض بر ثابت بودن

    # تنظیمات داخلی و هوشمند Scrapy
    custom_settings = {
        "FEEDS": {
            "Irna_Parametric_Dataset.csv": {
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
        "ROBOTSTXT_OBEY": False,  # <--- برای تست غیرفعال شد
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 5,
        "DOWNLOAD_TIMEOUT": 30,
        # --- تنظیمات Playwright ---
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,  # اجرای مرورگر در پس‌زمینه
        },
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 120000,
    }

    def __init__(self, *args, **kwargs):
        super(IrnaSpider, self).__init__(*args, **kwargs)
        # دریافت آرگومان‌ها از خط فرمان (kwargs) و تبدیل به integer
        self.max_month_test = int(
            kwargs.get("max_month_test", self.MAX_MONTH_TEST_DEFAULT)
        )
        self.max_day_test = int(kwargs.get("max_day_test", self.MAX_DAY_TEST_DEFAULT))
        self.max_pages_per_day = int(
            kwargs.get("max_pages_per_day", self.MAX_PAGES_PER_DAY_DEFAULT)
        )

        # لاگ برای تأیید تنظیمات فعلی
        self.logger.info(
            f"CRAWL TEST SETTINGS: Months={self.max_month_test}, Days={self.max_day_test}, Pages/Day={self.max_pages_per_day}"
        )

    # -------------------------------------------------------------------
    # توابع کمکی
    # -------------------------------------------------------------------

    def clean_rtl_chars(self, text):
        """حذف کاراکترهای جهت‌دهی (LRE, PDF, LRM, RLM)."""
        text = text.replace("\u200e", "").replace("\u200f", "")
        text = text.replace("\u202a", "").replace("\u202c", "")
        return text

    def convert_shamsi_to_georgian(self, shamsi_date_str):
        """تبدیل تاریخ شمسی (YYYY/MM/DD) به فرمت ISO میلادی."""
        try:
            # پاکسازی و تقسیم رشته
            shamsi_date_str = shamsi_date_str.split(" ")[0]  # حذف زمان اگر وجود دارد
            parts = shamsi_date_str.split("/")

            # تبدیل به jdate و سپس datetime
            jdate = jdatetime.date(int(parts[0]), int(parts[1]), int(parts[2]))
            gdate = jdate.togregorian()
            return gdate.isoformat()
        except Exception as e:
            self.logger.error(f"Failed to convert date '{shamsi_date_str}': {e}")
            return None

    def start_requests(self):
        """ایجاد درخواست‌های اولیه با حلقه زدن روی سال، ماه و روز (Date-Major)."""

        # از متغیرهای تست (self.max_month_test) استفاده می‌کند.
        for yr in range(self.START_YEAR, self.END_YEAR + 1):
            # محدود کردن ماه برای تست
            for mn in range(1, self.max_month_test + 1):
                # محدود کردن روز برای تست
                for dy in range(1, self.max_day_test + 1):
                    # حلقه دسته‌بندی‌ها (این حلقه اکنون درونی‌ترین است)
                    for category in self.CATEGORY_IDS:
                        cat_id = category["id"]
                        cat_name = category["name"]

                        # pi=1 به معنای صفحه اول است
                        url = f"{self.URL_BASE}pi=1&tp={self.TP}&ms={cat_id}&dy={dy}&mn={mn}&yr={yr}"

                        # ارسال درخواست به تابع parse_archive
                        yield scrapy.Request(
                            url=url,
                            callback=self.parse_archive,
                            meta={
                                "playwright": True,  # <--- فعال‌سازی Playwright برای رندر آرشیو
                                "category_name": cat_name,
                                "category_id": cat_id,
                                "day": dy,
                                "month": mn,
                                "year": yr,
                                "page": 1,
                            },
                        )

    def parse_archive(self, response):
        """تجزیه و تحلیل صفحه آرشیو (استخراج لینک‌ها و Pagination)."""

        cat_name = response.meta["category_name"]
        current_page = response.meta["page"]

        # 1. استخراج لینک‌های خبر
        # اصلاح: جستجوی جامع‌تر برای لینک‌های تیتر خبر در تگ‌های مختلف h1 تا h5
        news_links = response.css(
            "h1 a::attr(href), h2 a::attr(href), h3 a::attr(href), h4 a::attr(href), h5 a::attr(href)"
        ).getall()

        if not news_links:
            # اگر هیچ لینکی پیدا نشد، لاگ دیباگ ثبت می‌شود
            self.logger.debug(
                f"No news links found for {cat_name} on page {current_page} - {response.meta['year']}/{response.meta['month']}/{response.meta['day']}. Stopping pagination for this day."
            )
            return

        for link in news_links:
            full_url = response.urljoin(link)

            # ارسال لینک به تابع parse_news برای استخراج جزییات (بدون Playwright برای سرعت بیشتر)
            yield scrapy.Request(
                full_url, callback=self.parse_news, meta={"category_name": cat_name}
            )

        # 2. مدیریت Pagination (صفحه‌بندی)
        # فقط در صورتی که به حد تعیین شده نرسیده‌ایم، به صفحه بعدی می‌رویم
        if current_page < self.max_pages_per_day:
            next_page = current_page + 1

            # ساخت URL صفحه بعدی (جایگزین کردن پارامتر pi)
            new_url = re.sub(r"pi=\d+", f"pi={next_page}", response.url)

            # درخواست صفحه بعدی را ارسال می‌کنیم
            yield scrapy.Request(
                new_url,
                callback=self.parse_archive,
                meta={
                    "playwright": True,  # <--- فعال‌سازی Playwright برای رندر صفحه بعدی آرشیو
                    "category_name": cat_name,
                    "category_id": response.meta["category_id"],
                    "day": response.meta["day"],
                    "month": response.meta["month"],
                    "year": response.meta["year"],
                    "page": next_page,
                },
            )

    def parse_news(self, response):
        """تابع اصلی تجزیه و تحلیل صفحه خبر."""

        item = IrnaItem()
        item["category"] = response.meta.get("category_name", "نامشخص")
        item["link"] = response.url

        # 1. استخراج تیتر
        title = response.css("h1.title::text").get()
        if not title:
            self.logger.debug(
                f"Skipping news item - Title not found for URL: {response.url}"
            )
            return
        item["title"] = self.clean_rtl_chars(title.strip())

        # 2. استخراج خلاصه/لید (حدس بر اساس ساختار رایج)
        abstract = response.css("h3::text, div.lead::text").get() or ""
        item["abstract"] = self.clean_rtl_chars(abstract.strip())

        # 3. استخراج بدنه خبر (با توجه به تأیید کاربر: item-body -> item-text -> p)
        body_parts = response.css("div.item-body div.item-text p::text").getall()
        body = "\n".join(p.strip() for p in body_parts if p.strip())
        item["body"] = self.clean_rtl_chars(body)
        if not item["body"]:
            self.logger.debug(
                f"Skipping news item - Body not found for URL: {response.url}"
            )
            return

        # 4. استخراج تاریخ شمسی
        date_time_shamsi = (
            response.css(".item-date time::attr(datetime)").get()
            or response.css(".item-date::text").get()
        )

        # 5. تبدیل تاریخ شمسی به میلادی
        date_iso = None
        if date_time_shamsi:
            # تاریخ شمسی را برای تبدیل به تابع ارسال می‌کنیم (مثلاً 1404/07/13)
            date_iso = self.convert_shamsi_to_georgian(date_time_shamsi.split(" ")[0])

        item["date_georgian_iso"] = date_iso

        if not item["date_georgian_iso"]:
            self.logger.debug(
                f"Skipping news item - Date conversion failed for URL: {response.url}"
            )
            return

        yield item
