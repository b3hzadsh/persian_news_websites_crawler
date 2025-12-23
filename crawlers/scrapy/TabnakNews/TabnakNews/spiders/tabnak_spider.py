import locale
import re
import time
from urllib.parse import urlencode

import scrapy
from jdatetime import date as jdate, datetime as jdatetime, timedelta
from scrapy.item import Field, Item

crawl_start_date = "1400/01/03"
crawl_end_date = "1400/01/06"


class NewsItem(Item):
    body = Field()
    date_georgian_iso = Field()
    news_id = Field()
    category = Field()


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
    TARGET_CATEGORIES = [
        "ورزشی",
        "اقتصادی",
        "سیاسی",
    ]
    categories_str = "_".join(TARGET_CATEGORIES)
    filename = f"Tabnak_{categories_str}_{crawl_start_date.replace('/', '-')}_to_{crawl_end_date.replace('/', '-')}.csv"
    custom_settings = {
        "FEEDS": {
            filename: {
                "format": "csv",
                "encoding": "utf-8-sig",
                "overwrite": True,
            }
        },
        "FEED_EXPORT_ENCODING": "utf8",
        "CONCURRENT_REQUESTS": 16,
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
        # "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",  # بهبود: deduplication
        # "JOBDIR": "/tmp/scrapy_job",  # برای dedup
    }

    def __init__(self, *args, **kwargs):
        super(TabnakDailyCrawler, self).__init__(*args, **kwargs)
        self.start_time = time.time()  # زمان شروع
        self.from_date_str = kwargs.get("from_date", crawl_start_date).replace("/", "-")
        self.to_date_str = kwargs.get("to_date", crawl_end_date).replace("/", "-")

        try:
            locale.setlocale(locale.LC_TIME, "C")
        except locale.Error:
            self.logger.warning("Could not set locale to 'C'. Date parsing may fail.")

        try:
            jdatetime.strptime(self.from_date_str, "%Y-%m-%d")
            jdatetime.strptime(self.to_date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                "فرمت تاریخ اشتباه است. لطفاً از فرمت شمسی YYYY/MM/DD استفاده کنید."
            )

        self.logger.info(
            f"CRAWLING DAILY from {self.from_date_str} to {self.to_date_str} for categories: {self.TARGET_CATEGORIES}"
        )

    def start_requests(self):
        base_url = "https://www.tabnak.ir/fa/archive?"
        start_date = jdatetime.strptime(self.from_date_str, "%Y-%m-%d").date()
        end_date = jdatetime.strptime(self.to_date_str, "%Y-%m-%d").date()
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

        news_links = response.xpath(
            '//div[@class="linear_news"]//a[@class="title5"]/@href'
        ).getall()

        if not news_links:
            self.logger.warning(f"No news links found on archive page: {response.url}")

        for link in news_links:
            if response.meta.get("depth", 0) > 5:
                self.logger.warning("Max depth reached, skipping pagination.")
                break
            yield response.follow(
                link,
                callback=self.parse_news,
                meta={"depth": response.meta.get("depth", 0) + 1},
            )

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
            item["category"] = clean_persian_text(
                response.css("a.newsbody_servicename::text").get(default="")
            )
            id_match = re.search(r"/news/(\d+)", response.url)
            if id_match:
                item["news_id"] = id_match.group(1)
            else:
                item["news_id"] = None
                self.logger.warning(
                    f"Could not extract news ID from URL: {response.url}"
                )
            body_texts = response.xpath(
                '//div[@class="body"]//p[not(contains(@class, "ad")) and not(contains(@class, "footer"))]/text()'
            ).getall()
            cleaned_body = "\n".join(
                clean_persian_text(p) for p in body_texts if clean_persian_text(p)
            )
            item["body"] = cleaned_body

            item["date_georgian_iso"] = self.extract_and_convert_date(response)

            if not item["body"]:
                self.logger.warning(f"Incomplete item skipped: {response.url}")
                return
            count = self.crawler.stats.get_value("item_scraped_count", 0)
            if count and count % 10 == 0:
                elapsed = time.time() - self.start_time
                self.logger.info(
                    f"Elapsed time so far: {elapsed:.2f} seconds. Items scraped: {self.crawler.stats.get_value('item_scraped_count', 0)}"
                )

            yield item
        except Exception as e:
            self.logger.error(f"Error parsing news {response.url}: {str(e)}")

    def extract_and_convert_date(self, response):
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
        self.logger.info(
            f"Spider closed: {reason}. Processed items: {self.crawler.stats.get_value('item_scraped_count', 0)}"
        )


def clean_persian_text(text):
    if not text:
        return ""
    # حذف کاراکترهای نامرئی و کنترل
    text = re.sub(
        r"[\u200c\u200d\u200e\u200f\u061c\u202a-\u202f\u2066-\u2069]", "", text
    )  # نیم‌فاصله و RTL/LTR marks
    # جایگزینی چندین فضای سفید با یکی
    text = re.sub(r"\s+", " ", text)
    # حذف فضای ابتدا و انتها
    return text.strip()
