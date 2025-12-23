import scrapy
import re
import locale
from scrapy.item import Item, Field
from urllib.parse import urlencode
import time

# --- ایمپورت کردن کتابخانه تاریخ شمسی ---
from jdatetime import date as jdate, timedelta, datetime as jdatetime

crawl_start_date = "1390/01/01"
crawl_end_date = "1390/01/01"
# -------------------------------------------------------------
# 1. تعریف ساختار داده (Item) - حذف date_persian
# -------------------------------------------------------------
class NewsItem(Item):
    # title = Field()
    body = Field()
    date_georgian_iso = Field()
    link = Field()
    category = Field()

# -------------------------------------------------------------
# 2. تعریف Spider با بهبودهای پیشنهادی
# -------------------------------------------------------------
class TabnakDailyCrawler(scrapy.Spider):
    name = "tabnak_daily_crawler"
    allowed_domains = ["tabnak.ir"]

    CATEGORY_MAP = {
        "سیاسی": "24",
        "اقتصادی": "6",
        "ورزشی": "2",
        "اجتماعی": "3",
        "فرهنگی": "21",
        "بین‌الملل": "17",
    }
    TARGET_CATEGORIES = ["ورزشی", "اقتصادی", "سیاسی", "فرهنگی"]
    # TARGET_CATEGORIES = ["ورزشی", "اقتصادی", "سیاسی", "فرهنگی"]

    custom_settings = {
        "FEEDS": {
            "Tabnak_Daily_Dataset.csv": {
                "format": "csv",
                "encoding": "utf-8-sig",  # بهبود: برای سازگاری با Excel
                "overwrite": True,
            }
        },
        "FEED_EXPORT_ENCODING": "utf8",
        "CONCURRENT_REQUESTS": 16,  # بهبود: افزایش برای سرعت بیشتر
        "DOWNLOAD_DELAY": 0,
        "DOWNLOAD_TIMEOUT": 10,  # بهبود: timeout برای درخواست‌های کند
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 TabnakDailyCrawler/3.2",
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 8.0,  # بهبود: افزایش concurrency
        "RETRY_TIMES": 2,  # بهبود: retry خودکار
        "RETRY_HTTP_CODES": [500, 502, 503, 504],
        "HTTPCACHE_ENABLED": True,  # بهبود: caching برای سرعت
        "HTTPCACHE_STORAGE": "scrapy.extensions.httpcache.FilesystemCacheStorage",
        "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",  # بهبود: deduplication
        "JOBDIR": "/tmp/scrapy_job",  # برای dedup
    }

    def __init__(self, *args, **kwargs):
        super(TabnakDailyCrawler, self).__init__(*args, **kwargs)
        self.start_time = time.time()  # زمان شروع
        self.from_date_str = kwargs.get("from_date", crawl_start_date)
        self.to_date_str = kwargs.get("to_date", crawl_end_date)

        # --- بهبود: set locale یکبار ---
        try:
            locale.setlocale(locale.LC_TIME, "C")
        except locale.Error:
            self.logger.warning("Could not set locale to 'C'. Date parsing may fail.")

        # --- اعتبارسنجی با jdatetime ---
        try:
            jdatetime.strptime(self.from_date_str, "%Y/%m/%d")
            jdatetime.strptime(self.to_date_str, "%Y/%m/%d")
        except ValueError:
            raise ValueError(
                "فرمت تاریخ اشتباه است. لطفاً از فرمت شمسی YYYY/MM/DD استفاده کنید."
            )

        self.logger.info(
            f"CRAWLING DAILY from {self.from_date_str} to {self.to_date_str} for categories: {self.TARGET_CATEGORIES}"
        )

    def start_requests(self):
        # --- بهبود: batching همه URLها برای مدیریت بهتر scheduler ---
        base_url = "https://www.tabnak.ir/fa/archive?"
        start_date = jdatetime.strptime(self.from_date_str, "%Y/%m/%d").date()
        end_date = jdatetime.strptime(self.to_date_str, "%Y/%m/%d").date()
        urls = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y/%m/%d")
            self.logger.info(f"--- Generating requests for date: {date_str} ---")
            for category_name in self.TARGET_CATEGORIES:
                service_id = self.CATEGORY_MAP.get(category_name)
                if not service_id:
                    self.logger.warning(
                        f"Category '{category_name}' not found. Skipping."
                    )
                    continue
                params = {
                    "service_id": service_id,
                    "rpp": 100,
                    "from_date": date_str,
                    "to_date": date_str,
                }
                start_url = base_url + urlencode(params)
                urls.append(start_url)
            current_date += timedelta(days=1)

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={"depth": 0})

    def parse(self, response):
        """
        پردازشگر صفحات آرشیو با انتخابگرهای بهینه
        """
        self.logger.info(f"Parsing archive page: {response.url}")

        # --- بهبود: استفاده از XPath برای سرعت بیشتر ---
        news_links = response.xpath(
            '//div[@class="linear_news"]//a[@class="title5"]/@href'
        ).getall()

        if not news_links:
            self.logger.warning(f"No news links found on archive page: {response.url}")

        for link in news_links:
            # --- بهبود: چک depth برای جلوگیری از chain طولانی ---
            if response.meta.get("depth", 0) > 5:
                self.logger.warning("Max depth reached, skipping pagination.")
                break
            yield response.follow(
                link,
                callback=self.parse_news,
                meta={"depth": response.meta.get("depth", 0)},
            )

        # --- pagination با چک depth ---
        next_page_link = response.xpath(
            '//div[contains(@class, "pagination")]//a[contains(text(), "»")]/@href'
        ).get()

        if next_page_link and response.meta.get("depth", 0) < 5:
            self.logger.info(f"Found next page link: {next_page_link}")
            yield response.follow(
                next_page_link,
                callback=self.parse,
                meta={"depth": response.meta.get("depth", 0) + 1},
            )

    def parse_news(self, response):
        item = NewsItem()
        try:
            # --- استخراج با try-except برای مدیریت خطا ---
            item["category"] = (
                response.css("a.newsbody_servicename::text").get(default="").strip()
            )
            id_match = re.search(r"/news/(\d+)", response.url)
            if id_match:
                item["link"] = id_match.group(1)  # فقط ID عددی
            else:
                item["link"] = None  # یا "" اگر ترجیح می‌دی
                self.logger.warning(
                    f"Could not extract news ID from URL: {response.url}"
                )

            # item["title"] = (
            #     response.css("h1.Htag::text, h1.title::text").get(default="").strip()
            # )

            # --- بهبود: استخراج بدنه دقیق‌تر با XPath و فیلتر نویز ---
            body_texts = response.xpath(
                '//div[@class="body"]//p[not(contains(@class, "ad")) and not(contains(@class, "footer"))]/text()'
            ).getall()
            item["body"] = "\n".join(p.strip() for p in body_texts if p.strip())

            item["date_georgian_iso"] = self.extract_and_convert_date(response)

            # --- بهبود: validation ساده ---
            if not item["body"]:
            # if not item["title"] or not item["body"]:
                self.logger.warning(f"Incomplete item skipped: {response.url}")
                return
            if self.crawler.stats.get_value("item_scraped_count", 0) % 10 == 0:
                elapsed = time.time() - self.start_time
                self.logger.info(
                    f"Elapsed time so far: {elapsed:.2f} seconds. Items scraped: {self.crawler.stats.get_value('item_scraped_count', 0)}"
                )

            yield item
        except Exception as e:
            self.logger.error(f"Error parsing news {response.url}: {str(e)}")

    def extract_and_convert_date(self, response):
        # --- بهبود: مدیریت خطا و log ---
        date_en_tag = response.css("span.en_date::text").get()
        if date_en_tag:
            try:
                from datetime import datetime as py_datetime

                date_obj = py_datetime.strptime(date_en_tag.strip(), "%d %B %Y")
                return date_obj.date().isoformat()
            except (ValueError, locale.Error) as e:
                self.logger.warning(
                    f"Failed to parse date '{date_en_tag}' for {response.url}: {str(e)}"
                )
        else:
            self.logger.warning(f"No date tag found for {response.url}")
        return None

    def closed(self, reason):
        # --- بهبود: آمار نهایی با signals ---
        self.logger.info(
            f"Spider closed: {reason}. Processed items: {self.crawler.stats.get_value('item_scraped_count', 0)}"
        )
