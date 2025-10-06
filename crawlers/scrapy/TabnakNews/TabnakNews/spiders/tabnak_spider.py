import scrapy
import re
import locale
from datetime import datetime
from scrapy.item import Item, Field
from urllib.parse import urlencode

# -------------------------------------------------------------
# 1. تعریف ساختار داده (Item) - بدون تغییر
# -------------------------------------------------------------
class NewsItem(Item):
    title = Field()
    abstract = Field()
    body = Field()
    date_georgian_iso = Field()
    link = Field()
    category = Field()

# -------------------------------------------------------------
# 2. تعریف Spider با قابلیت دریافت آرگومان
# -------------------------------------------------------------
class TabnakArchiveSpider(scrapy.Spider):
    """
    اسپایدر پیشرفته تابناک که دسته بندی، بازه زمانی و تعداد نتایج را
    به عنوان آرگومان از خط فرمان دریافت می کند.
    """
    name = "tabnak_archive_crawler"
    allowed_domains = ["tabnak.ir"]

    # --- دیکشنری برای تبدیل نام فارسی دسته بندی به ID (برای راحتی کاربر) ---
    CATEGORY_MAP = {
        "سیاسی": "2",
        "اقتصادی": "3",
        "ورزشی": "6",
        "اجتماعی": "4",
        "فرهنگی": "5",
        "بین‌الملل": "8",
        # ... می توانید دسته بندی های دیگر را اضافه کنید
    }

    # تنظیمات داخلی Scrapy (بدون تغییر)
    custom_settings = {
        "FEEDS": {
            "Tabnak_Scrapy_Dataset.csv": { "format": "csv", "encoding": "utf8", "overwrite": True }
        },
        # ... سایر تنظیمات قبلی پابرجا هستند ...
        "FEED_EXPORT_ENCODING": "utf8",
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_DELAY": 0,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 TabnakDynamicCrawler/2.0",
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 3.0,
    }

    def __init__(self, *args, **kwargs):
        """
        این تابع آرگومان های ورودی از خط فرمان را می خواند.
        """
        super(TabnakArchiveSpider, self).__init__(*args, **kwargs)

        # خواندن آرگومان ها با مقادیر پیش فرض
        self.from_date = kwargs.get("from_date", "1403/01/01")
        self.to_date = kwargs.get("to_date", "1403/01/10")
        self.rpp = kwargs.get("rpp", "50")

        # خواندن دسته بندی ها (می تواند چند دسته بندی با کاما جدا شده باشد)
        # مثال: categories="ورزشی,اقتصادی"
        categories_input = kwargs.get("categories", "ورزشی")
        
        self.service_ids = []
        for cat_name in categories_input.split(','):
            cat_name = cat_name.strip()
            # تبدیل نام فارسی به ID (اگر در دیکشنری بود)
            service_id = self.CATEGORY_MAP.get(cat_name, cat_name)
            self.service_ids.append(service_id)
        
        self.logger.info(f"CRAWLING PARAMETERS:")
        self.logger.info(f"  - Categories (IDs): {self.service_ids}")
        self.logger.info(f"  - Date Range: {self.from_date} to {self.to_date}")
        self.logger.info(f"  - Results Per Page: {self.rpp}")


    def start_requests(self):
        """
        این تابع بر اساس آرگومان های ورودی، URL های اولیه را تولید و ارسال می کند.
        """
        base_url = "https://www.tabnak.ir/fa/archive?"
        
        # به ازای هر دسته بندی انتخاب شده، یک URL جداگانه می سازیم
        for service_id in self.service_ids:
            params = {
                'service_id': service_id,
                'sec_id': -1,
                'cat_id': -1,
                'rpp': self.rpp,
                'from_date': self.from_date,
                'to_date': self.to_date
            }
            # ساخت URL کامل با پارامترها
            start_url = base_url + urlencode(params)
            self.logger.info(f"Starting crawl for URL: {start_url}")
            yield scrapy.Request(url=start_url, callback=self.parse)


    def parse(self, response):
        """
        پردازشگر صفحات آرشیو (بدون تغییر)
        """
        # 1. پیدا کردن لینک های خبر
        news_links = response.css("div.archive_list_media h3 a::attr(href)").getall()
        for link in news_links:
            yield response.follow(link, callback=self.parse_news)

        # 2. پیدا کردن لینک صفحه بعد
        next_page_link = response.css("a.news_next::attr(href)").get()
        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse)
            
            
    def parse_news(self, response):
        """
        استخراج کننده اطلاعات از صفحه خبر (بدون تغییر)
        """
        item = NewsItem()
        item["category"] = response.css("a.newsbody_servicename::text").get(default="").strip()
        
        id_match = re.search(r"/news/(\d+)", response.url)
        item["link"] = id_match.group(1) if id_match else response.url
        
        item["title"] = response.css("h1.Htag::text, h1.title::text").get(default="").strip()
        item["abstract"] = response.css("div.subtitle::text, div.lead::text").get(default="").strip()
        
        body_parts = response.css("div.body p::text, div.body div.rte > *::text").getall()
        item["body"] = "\n".join(p.strip() for p in body_parts if p.strip())
        
        item["date_georgian_iso"] = self.extract_and_convert_date(response)

        # تمیزکاری نهایی متون
        for field in ['title', 'abstract', 'body']:
            if item[field]:
                item[field] = self.clean_rtl_chars(item[field])

        yield item

    # توابع کمکی (بدون تغییر)
    def clean_rtl_chars(self, text):
        return text.replace("\u200e", "").replace("\u200f", "").replace("\u