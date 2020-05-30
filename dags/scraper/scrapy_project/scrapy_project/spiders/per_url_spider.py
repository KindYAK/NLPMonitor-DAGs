import scrapy

from .utils import parse_response


class PerUrlSpider(scrapy.spiders.Spider):
    name = "per_url_spider"
    custom_settings = {
        'DEPTH_LIMIT': 1,
        'DEPTH_PRIORITY': 1
    }
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
        url_filename = kw['url_filename']
        self.start_urls = []
        self.perform_full = True
        with open(url_filename, "r", encoding='utf-8') as f:
            for url in f.readlines():
                self.start_urls.append(url)

    def parse(self, response):
        return parse_response(self, response)
