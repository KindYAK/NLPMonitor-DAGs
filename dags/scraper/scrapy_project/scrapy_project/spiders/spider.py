import scrapy
import datetime
import pytz
import dateparser

from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from scrapy_splash import SplashJsonResponse, SplashTextResponse


class TheSpider(scrapy.spiders.CrawlSpider):
    name = "spider"
    custom_settings = {
        'DEPTH_LIMIT': 100,
        'DEPTH_PRIORITY': 1
    }
    rules = (scrapy.spiders.Rule(LinkExtractor(),
                                 callback="parse_item",
                                 follow=True,
                                 # process_request="splash_request"
                                 ),
             )

    def splash_request(self, request):
        request.meta.update(splash={
            'args': {
                'wait': 1,
            },
            'endpoint': 'render.html',
        })
        return request

    # def _requests_to_follow(self, response):
    #     if not isinstance(
    #             response,
    #             (HtmlResponse, SplashJsonResponse, SplashTextResponse)):
    #         return
    #     seen = set()
    #     for n, rule in enumerate(self._rules):
    #         links = [lnk for lnk in rule.link_extractor.extract_links(response)
    #                  if lnk not in seen]
    #         if links and rule.process_links:
    #             links = rule.process_links(links)
    #         for link in links:
    #             seen.add(link)
    #             r = self._build_request(n, link)
    #             yield rule.process_request(r)

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)

        self.start_urls = [kw['url']]
        self.allowed_domains = [kw['url'].split("/")[2]]
        self.latest_date = datetime.datetime.strptime(kw['latest_date'][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Almaty'))
        self.last_depth = None
        self.depth_history = []
        self.depth_history_depth = 1
        self.start_time = datetime.datetime.now()

    def parse_item(self, response):
        if not "text" in response.headers['Content-Type'].decode('utf-8'):
            return None
        # if not hasattr(self, "i"):
        #     self.i = 0
        # if self.i > 4:
        #     raise CloseSpider('No more new stuff')
        if self.depth_history_depth < response.meta['depth']:
            fails_ratio = sum(self.depth_history) / len(self.depth_history) if len(self.depth_history) else 0
            if fails_ratio > 0.9 and not self.last_depth:
                self.last_depth = response.meta['depth'] + 1
            self.depth_history_depth = response.meta['depth']
            self.depth_history = []
        if self.last_depth and response.meta['depth'] > self.last_depth or (datetime.datetime.now() - self.start_time).seconds > 24*60*60:
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
                try:
                    parse_result = parse_result.lower().replace("опубликовано:", "").replace("автор", "")
                    parse_result = dateparser.parse(parse_result, languages=['ru']).replace(tzinfo=pytz.timezone('Asia/Almaty'))
                    if parse_result.year < 2000:
                        return None
                except:
                    return None
                date_now = datetime.datetime.now().date()
                if parse_result.year == date_now.year and parse_result.month > date_now.month:
                    parse_result.year = parse_result.year - 1
                if parse_result < self.latest_date:
                    self.depth_history.append(1)
                else:
                    self.depth_history.append(0)
            if field.startswith("num_"):
                try:
                    parse_result = int(parse_result)
                except:
                    parse_result = 0
            result[field] = parse_result
        result['url'] = response.__dict__['_url']
        result['html'] = "\n".join(response.css(self.text).extract())
        result['datetime_created'] = datetime.datetime.now().replace(tzinfo=pytz.timezone('Asia/Almaty'))
        # self.i += 1
        # print("!!!", self.i, response.meta['depth'])
        # print(result['datetime'], self.latest_date, self.last_depth)
        yield result
