import scrapy
import re
import locale
from datetime import datetime
from scrapy.item import Item, Field


# -------------------------------------------------------------
# 1. تعریف ساختار داده (Item)
# -------------------------------------------------------------
class NewsItem(Item):
    """مدل داده خروجی برای ذخیره سازی در CSV."""

    title = Field()
    abstract = Field()
    body = Field()
    date_georgian_iso = Field()
    link = Field()
    category = Field()  # فیلد جدید برای ذخیره دسته بندی خبر


# -------------------------------------------------------------
# 2. تعریف Spider
# -------------------------------------------------------------
class TabnakSpider(scrapy.Spider):
    """
    Scrapy Spider برای کراول کردن سایت تابناک با استفاده از ID متوالی اخبار.
    این اسپایدر آرگومان های start_id و end_id را از خط فرمان دریافت می کند.
    """

    name = "tabnak_id_crawler_scrapy"
    allowed_domains = ["tabnak.ir"]

    # ***تغییر رنج پیش فرض به IDهای جدیدتر برای تضمین کارکرد انتخابگرها***
    START_ID_DEFAULT = 1200000
    END_ID_DEFAULT = 1200500

    # تعریف آرگومان ها به عنوان None تا مشخص شود باید از خط فرمان دریافت شوند
    start_id = None
    end_id = None

    # تنظیمات داخلی و هوشمند Scrapy
    custom_settings = {
        # **تنظیمات خروجی: این قسمت تضمین می کند که داده ها در فایل CSV ذخیره شوند**
        "FEEDS": {
            "Tabnak_Scrapy_Dataset.csv": {  # نام و مسیر فایل خروجی (در پوشه اصلی پروژه ایجاد می شود)
                "format": "csv",
                "encoding": "utf8",  # استفاده از UTF8 برای پشتیبانی کامل از کاراکترهای فارسی
                "store_empty": False,
                "overwrite": False,  # اگر True باشد، فایل موجود را بازنویسی می کند
                # اضافه کردن 'category' به لیست فیلدهای خروجی
                "fields": [
                    "title",
                    "abstract",
                    "body",
                    "date_georgian_iso",
                    "link",
                    "category",
                ],
            }
        },
        "FEED_EXPORT_ENCODING": "utf8",  # تنظیم جهانی برای اطمینان از خروجی فارسی
        # **فعال سازی سیستم AutoThrottle**
        # 'AUTOTHROTTLE_ENABLED': True,
        # 'AUTOTHROTTLE_START_DELAY': 1.0,
        # 'AUTOTHROTTLE_MAX_DELAY': 10.0,
        # 'AUTOTHROTTLE_TARGET_CONCURRENCY': 5.0,
        # **تنظیمات همزمانی**
        "CONCURRENT_REQUESTS": 16,  # حداکثر درخواست‌های همزمان (پایین‌تر از 100 برای شروع)
        "DOWNLOAD_DELAY": 0,  # AutoThrottle این را نادیده می‌گیرد اما باید 0 باشد.
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 EntekhabCrawler/1.0",
        "ROBOTSTXT_OBEY": False,  # برای IRNA و Entekhab که مشکل رندر دارند، بهتر است موقتاً خاموش باشد.
        # تنظیمات AutoThrottle (که اهمیت بالاتری دارند)
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 3.0,  # تنظیم کلیدی: نرخ بهینه را روی یک عدد محافظه‌کارانه (3 تا 5) تنظیم کنید.
        "AUTOTHROTTLE_START_DELAY": 2.0,  # تأخیر شروع را کمی بیشتر می‌کنیم تا سرور به ربات عادت کند.
    }

    def __init__(self, *args, **kwargs):
        super(TabnakSpider, self).__init__(*args, **kwargs)

        # دریافت آرگومان ها از خط فرمان و تبدیل به عدد صحیح
        try:
            # self.start_id و self.end_id از آرگومان های خط فرمان (kwargs) دریافت می شوند
            self.start_id = int(kwargs.get("start_id", self.START_ID_DEFAULT))
            self.end_id = int(kwargs.get("end_id", self.END_ID_DEFAULT))
            self.logger.info(
                f"Crawl range set from command line: {self.start_id} to {self.end_id}"
            )
        except ValueError:
            self.logger.error("Error: start_id and end_id must be valid integers.")
            # اگر خطایی رخ داد، از مقادیر پیش فرض استفاده کند
            self.start_id = self.START_ID_DEFAULT
            self.end_id = self.END_ID_DEFAULT

    def clean_rtl_chars(self, text):
        """
        کاراکترهای جهت دهی (LRE, PDF, LRM, RLM) را حذف می کند.
        U+202A (LRE), U+202C (PDF), U+200E (LRM), U+200F (RLM)
        """
        text = text.replace("\u200e", "").replace("\u200f", "")
        text = text.replace("\u202a", "").replace("\u202c", "")
        return text

    def extract_and_convert_date(self, response):
        """تاریخ میلادی را استخراج و به فرمت ISO YYYY-MM-DD تبدیل می کند."""
        # ... (منطق استخراج تاریخ بدون تغییر ماند)
        date_en_tag = response.css("span.en_date::text").get()
        date_iso = None

        if date_en_tag:
            raw_date = date_en_tag.strip()

            try:
                current_locale = locale.getlocale(locale.LC_TIME)
                locale.setlocale(locale.LC_TIME, "C")

                cleaned_date = re.sub(r"\s+", " ", raw_date).strip()

                date_obj = datetime.strptime(cleaned_date, "%d %B %Y")
                date_iso = date_obj.date().isoformat()

            except ValueError:
                self.logger.warning(
                    f"Date format error for URL {response.url}: '{raw_date}'"
                )
            finally:
                locale.setlocale(locale.LC_TIME, current_locale)

        return date_iso

    def start_requests(self):
        """ایجاد تمام درخواست های اولیه در بازه ID مشخص شده توسط آرگومان ها."""
        URL_BASE = "https://www.tabnak.ir/fa/news/"

        # استفاده از IDهایی که در __init__ تعریف شدند
        self.logger.info(f"Starting crawl for range: {self.start_id} to {self.end_id}")
        for news_id in range(self.start_id, self.end_id):
            url = URL_BASE + str(news_id)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """تابع اصلی تجزیه و تحلیل صفحه خبر."""

        item = NewsItem()
        allowed_categories = ["ورزشی", "اقتصادی", "سیاسی", "فرهنگی"]

        # استخراج دسته بندی (قبل از فیلتر کردن)
        category_tag = response.css("a.newsbody_servicename::text").get()

        if category_tag:
            category_name = category_tag.strip()
            # ذخیره دسته بندی در آیتم (صرف نظر از اینکه فیلتر می شود یا نه)
            item["category"] = category_name

            # فیلتر کردن خبر
            if category_name not in allowed_categories:
                self.logger.debug(
                    f"Skipping {response.url}: Category '{category_name}' is not in allowed list."
                )
                return  # نادیده گرفتن خبر و توقف پردازش
        else:
            self.logger.debug(
                f"Skipping {response.url}: Could not find news category tag."
            )
            return  # اگر تگ دسته بندی پیدا نشد، نادیده بگیر

        # ادامه پردازش فقط برای خبرهای مجاز

        # *** اصلاح: استخراج فقط ID از URL و ذخیره در item['link'] ***
        id_match = re.search(r"/news/(\d+)", response.url)
        if id_match:
            item["link"] = id_match.group(1)
        else:
            self.logger.warning(
                f"Could not reliably extract ID from URL: {response.url}. Using raw URL for link field."
            )
            item["link"] = response.url

        # 1. استخراج و تمیزسازی تیتر
        title = response.css("h1.Htag::text, h1.title::text").get()
        if not title:
            self.logger.debug(f"Skipping {response.url}: Title selector failed.")
            return
        item["title"] = self.clean_rtl_chars(title.strip())

        # 2. استخراج خلاصه/لید
        abstract = response.css("div.subtitle::text, div.lead::text").get() or ""
        item["abstract"] = self.clean_rtl_chars(abstract.strip())

        # 3. استخراج بدنه خبر
        body_parts = response.css(
            "div.body p::text, div.body div.rte > *::text"
        ).getall()
        body = "\n".join(p.strip() for p in body_parts if p.strip())
        item["body"] = self.clean_rtl_chars(body)
        if not item["body"]:
            self.logger.debug(f"Skipping {response.url}: Body content is empty.")
            return

        # 4. تبدیل و استخراج تاریخ
        date_iso = self.extract_and_convert_date(response)
        if not date_iso:
            self.logger.debug(f"Skipping {response.url}: Date extraction failed.")
            return
        item["date_georgian_iso"] = date_iso

        yield item
