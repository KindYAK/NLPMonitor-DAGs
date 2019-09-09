import scrapy
import datetime
import pytz
import dateparser

from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor
from scrapy_splash import SplashRequest


class TheSpider(scrapy.spiders.CrawlSpider):
    name = "spider"
    custom_settings = {
        'DEPTH_LIMIT': 50,
        'DEPTH_PRIORITY': 1
    }
    rules = (scrapy.spiders.Rule(LinkExtractor(),
                                 callback="parse_item",
                                 follow=True,
                                 process_request="splash_request"
                                 ),
             )

    def splash_request(self, request):
        return SplashRequest(request.url, self.parse_item, endpoint="render.html", args={'wait': 3}, meta={'real_url': request.url})

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

        self.start_urls = [kw['url']]
        self.allowed_domains = [kw['url'].replace("https://", "").replace("http://", "").replace("/", "")]
        self.latest_date = datetime.datetime.strptime(kw['latest_date'][:-6], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Almaty'))
        self.last_depth = None
        self.depth_history = []
        self.depth_history_depth = 1

    def parse_item(self, response):
        # if not hasattr(self, "i"):
        #     self.i = 0
        # if self.i > 4:
        #     raise CloseSpider('No more new stuff')
        if self.depth_history_depth < response.meta['depth']:
            fails_ratio = sum(self.depth_history) / len(self.depth_history)
            if fails_ratio > 0.95 and not self.last_depth:
                self.last_depth = response.meta['depth'] + 1
            self.depth_history_depth = response.meta['depth']
            self.depth_history = []
        if self.last_depth and response.meta['depth'] > self.last_depth:
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
                parse_result = dateparser.parse(parse_result, languages=['ru']).replace(tzinfo=pytz.timezone('Asia/Almaty'))
                if parse_result < self.latest_date:
                    self.depth_history.append(1)
                else:
                    self.depth_history.append(0)
            if field.startswith("num_"):
                parse_result = int(parse_result)
            result[field] = parse_result
        result['url'] = response.meta['real_url']
        result['html'] = "\n".join(response.css(self.text).extract())
        result['datetime_created'] = datetime.datetime.now().replace(tzinfo=pytz.timezone('Asia/Almaty'))
        # self.i += 1
        # print("!!!", self.i, response.meta['depth'])
        # print(result['datetime'], self.latest_date, self.last_depth)
        yield result
