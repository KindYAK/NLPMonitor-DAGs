import datetime

import pytz
import scrapy
from scrapy.linkextractors import LinkExtractor

from .utils import parse_response


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
    rotate_user_agent = True

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
        self.perform_full = kw['perform_full'] == "yes"
        self.last_depth = kw.get("max_depth", None)
        self.perform_fast = bool(kw.get("max_depth", False))
        if self.last_depth:
            self.last_depth = int(self.last_depth)
        self.depth_history = []
        self.depth_history_depth = 1
        self.start_time = datetime.datetime.now()

    def parse_item(self, response):
        return parse_response(self, response)
