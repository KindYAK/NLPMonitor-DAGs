import scrapy
import datetime
import dateparser

from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor


class TheSpider(scrapy.spiders.CrawlSpider):
    name = "spider"
    custom_settings = {
        'DEPTH_LIMIT': 50,
        'DEPTH_PRIORITY': 1
    }
    rules = (scrapy.spiders.Rule(LinkExtractor(), callback="parse_item", follow=True), )

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

        self.start_urls = [kw['url'],]
        self.allowed_domains = [kw['url'].replace("https://", "").replace("http://", "").replace("/", "")]
        self.latest_date = datetime.datetime.strptime(kw['latest_date'][:-6], "%Y-%m-%dT%H:%M:%S")
        self.last_depth = 50 # change to value from custom_settings

    def parse_item(self, response):
        # if not hasattr(self, "i"):
        #     self.i = 0
        # if self.i > 4:
        #     raise CloseSpider('No more new stuff')
        if response.meta['depth'] > self.last_depth:
            raise CloseSpider('No more new stuff')

        simple_fields = ("text", "title", "author", "datetime", "num_views", "num_likes", "num_comments", "num_shares", )
        complex_fields = ("tags", "categories", )

        result = {}
        for field in simple_fields:
            if not hasattr(self, field):
                continue
            html = response.css(getattr(self, field) + " ::text").extract()
            html = "\n".join(html)
            parse_result = html.strip()

            if field == "text" and (not parse_result or len(parse_result) < 10):
                return None
            if field == "datetime":
                parse_result = dateparser.parse(parse_result, languages=['ru'])
                if parse_result < self.latest_date and response.meta['depth'] < self.last_depth:
                    self.last_depth = response.meta['depth']
            result[field] = parse_result
        result['url'] = response.request.url
        result['html'] = "\n".join(response.css(self.text).extract())
        result['datetime_created'] = datetime.datetime.now()
        # self.i += 1
        yield result
