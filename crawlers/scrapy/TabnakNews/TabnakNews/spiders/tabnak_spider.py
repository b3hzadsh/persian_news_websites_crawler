import scrapy
import re
import locale
from scrapy.item import Item, Field
from urllib.parse import urlencode

# --- تغییر کلیدی: ایمپورت کردن کتابخانه تاریخ شمسی ---
from jdatetime import date as jdate, timedelta, datetime as jdatetime


# -------------------------------------------------------------
# 1. تعریف ساختار داده (Item) - بدون تغییر
# -------------------------------------------------------------
class NewsItem(Item):
    title = Field()
    # abstract = Field()
    body = Field()
    date_georgian_iso = Field()
    link = Field()
    category = Field()


# -------------------------------------------------------------
# 2. تعریف Spider با منطق صحیح تاریخ شمسی
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

    custom_settings = {
        "FEEDS": {
            "Tabnak_Daily_Dataset.csv": {
                "format": "csv",
                "encoding": "utf8",
                # "encoding": "utf-8-sig",
                "overwrite": True,
            }
        },
        "FEED_EXPORT_ENCODING": "utf8",
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_DELAY": 0,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 TabnakDailyCrawler/3.1",
        "ROBOTSTXT_OBEY": False,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
    }

    def __init__(self, *args, **kwargs):
        super(TabnakDailyCrawler, self).__init__(*args, **kwargs)
        self.from_date_str = kwargs.get("from_date", "1403/07/01")
        self.to_date_str = kwargs.get("to_date", "1403/07/05")

        # --- تغییر کلیدی: اعتبارسنجی با jdatetime ---
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
        base_url = "https://www.tabnak.ir/fa/archive?"

        # --- تغییر کلیدی: استفاده از jdate برای پیمایش تاریخ شمسی ---
        start_date = jdatetime.strptime(self.from_date_str, "%Y/%m/%d").date()
        end_date = jdatetime.strptime(self.to_date_str, "%Y/%m/%d").date()

        current_date = start_date
        # timedelta نیز از کتابخانه jdatetime ایمپورت شده است
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
                yield scrapy.Request(url=start_url, callback=self.parse)

            # این عملیات اکنون کاملا بر اساس تقویم شمسی صحیح است
            current_date += timedelta(days=1)

    # توابع parse و parse_news بدون هیچ تغییری باقی می مانند
    def parse(self, response):
        """
        پردازشگر صفحات آرشیو با انتخابگرهای صحیح و نهایی
        """
        self.logger.info(f"Parsing archive page: {response.url}")

        # --- تغییر نهایی: استفاده از انتخابگر دقیق بر اساس HTML شما ---
        # انتخابگر صحیح: 'div.linear_news a.title5::attr(href)'
        news_links = response.css("div.linear_news a.title5::attr(href)").getall()

        if not news_links:
            self.logger.warning(f"No news links found on archive page: {response.url}")

        for link in news_links:
            yield response.follow(link, callback=self.parse_news)

        # انتخابگر صفحه بعد (این بخش احتمالا صحیح است و نیازی به تغییر ندارد)
        next_page_link = response.xpath(
            '//div[contains(@class, "pagination")]//a[contains(text(), "»")]/@href'
        ).get()

        if next_page_link:
            self.logger.info(f"Found next page link: {next_page_link}")
            yield response.follow(next_page_link, callback=self.parse)

    def parse_news(self, response):
        item = NewsItem()
        item["category"] = (
            response.css("a.newsbody_servicename::text").get(default="").strip()
        )
        id_match = re.search(r"/news/(\d+)", response.url)
        item["link"] = id_match.group(1) if id_match else response.url
        item["title"] = (
            response.css("h1.Htag::text, h1.title::text").get(default="").strip()
        )
        # item["abstract"] = (
        #     response.css("div.subtitle::text, div.lead::text").get(default="").strip()
        # )
        body_parts = response.css(
            "div.body p::text, div.body div.rte > *::text"
        ).getall()
        item["body"] = "\n".join(p.strip() for p in body_parts if p.strip())

        # این تابع تاریخ میلادی را از صفحه استخراج می کند و نیازی به تغییر ندارد
        item["date_georgian_iso"] = self.extract_and_convert_date(response)
        yield item

    def extract_and_convert_date(self, response):
        # این تابع تاریخ میلادی را از متن صفحه خبر می خواند، پس منطق آن صحیح است
        date_en_tag = response.css("span.en_date::text").get()
        if date_en_tag:
            try:
                # برای تبدیل تاریخ میلادی، همچنان از کتابخانه استاندارد استفاده می کنیم
                from datetime import datetime as py_datetime

                locale.setlocale(locale.LC_TIME, "C")
                date_obj = py_datetime.strptime(date_en_tag.strip(), "%d %B %Y")
                return date_obj.date().isoformat()
            except (ValueError, locale.Error):
                pass
        return None
