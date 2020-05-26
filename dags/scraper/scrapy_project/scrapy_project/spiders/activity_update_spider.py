import scrapy
import datetime
import pytz
import dateparser

from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from scrapy_splash import SplashJsonResponse, SplashTextResponse


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def normalize_url(url):
    url = url.replace("https://", "").replace("http://", "")
    url = url.replace("/", "")
    return url


class TheActivityUpdateSpider(scrapy.spiders.Spider):
    name = "activity_update_spider"
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
        self.urls_dict = {}
        self.start_urls = []
        with open(url_filename, "r", encoding='utf-8') as f:
            for lines in chunks(f.readlines(), 4):
                id, url, rule_views, rule_comments = lines
                id = id.strip()
                url = normalize_url(url.strip())
                rule_views = rule_views.strip()
                rule_comments = rule_comments.strip()
                self.urls_dict[url] = {
                    "id": id,
                    "rule_views": rule_views if rule_views != "None" else None,
                    "rule_comments": rule_comments if rule_comments != "None" else None,
                }
                self.start_urls.append(url)

    def parse(self, response):
        if not "text" in response.headers['Content-Type'].decode('utf-8'):
            return None

        url = response.request.url.strip()
        try:
            meta = self.urls_dict[normalize_url(url)]
        except:
            print("Missing url:", url, "Normalized url:", normalize_url(url))
            return None

        result = {}
        if meta['rule_views']:
            html = response.css(meta['rule_views'] + " ::text").extract()
            html = "\n".join(html)
            parse_result = html.strip()
            try:
                parse_result = int(parse_result)
            except:
                parse_result = 0
            result['num_views'] = parse_result
        if meta['rule_comments']:
            html = response.css(meta['rule_comments'] + " ::text").extract()
            html = "\n".join(html)
            parse_result = html.strip()
            try:
                parse_result = int(parse_result)
            except:
                parse_result = 0
            result['num_comments'] = parse_result
        result['id'] = meta['id']
        yield result
