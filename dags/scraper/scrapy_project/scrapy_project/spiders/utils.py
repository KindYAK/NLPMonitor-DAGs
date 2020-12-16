import datetime

import dateparser
import pytz
from scrapy.exceptions import CloseSpider


def parse_response(self, response):
    if not "text" in response.headers['Content-Type'].decode('utf-8'):
        return None
    # if not hasattr(self, "i"):
    #     self.i = 0
    # if self.i > 50:
    #     raise CloseSpider('No more new stuff')
    if not self.perform_full and not self.last_depth and self.depth_history_depth < response.meta['depth']:
        fails_ratio = sum(self.depth_history) / len(self.depth_history) if len(self.depth_history) else 0
        if fails_ratio > 0.9:
            self.last_depth = response.meta['depth'] + 1
        self.depth_history_depth = response.meta['depth']
        self.depth_history = []
    depth_exceeded = self.last_depth and response.meta['depth'] > self.last_depth
    if self.perform_fast:
        max_runtime = 30 * 60
    else:
        max_runtime = 12 * 60 * 60
    runtime_exceeded = (datetime.datetime.now() - self.start_time).seconds > max_runtime
    if not self.perform_full and (depth_exceeded or runtime_exceeded):
        raise CloseSpider('No more new stuff')

    simple_fields = ("text", "title", "author", "datetime", "num_views", "num_likes", "num_comments", "num_shares",)
    complex_fields = ("tags", "categories",)

    url = response.__dict__['_url']
    if "/kg/" in url or "/uz/" in url:
        return None
    if "dw.com" in url and "dw.com/ru/" not in url:
        return None
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
                if "dw.com" in url:
                    parse_result = parse_result.lower().strip().split("дата")[1].split("автор")[0].strip()
                if "bfm.ru" in url:
                    parse_result = " ".join(parse_result.lower().strip().split()[:4])
                parse_result = parse_result.lower().strip() \
                    .replace("опубликовано:", "") \
                    .replace("автор:", "") \
                    .replace("автор", "") \
                    .replace("мск", "") \
                    .replace("(-ов)", "")
                parse_result = dateparser.parse(parse_result, languages=['ru']).replace(
                    tzinfo=pytz.timezone('Asia/Almaty'))
                if not parse_result:
                    parse_result = dateparser.parse(" ".join(parse_result.split()[:-1]).strip(),
                                                    languages=['ru']).replace(tzinfo=pytz.timezone('Asia/Almaty'))
                if parse_result and parse_result.year < 2000:
                    continue
            except:
                safe_url = "".join(c for c in url.strip() if c.lower() in "1234567890-_qwertyuiopasdfghjklzxcvbnm?/\!@#$%^&*()_+><,.,;][}{")
                print("Datetime parse Exception", safe_url)
                continue
            date_now = datetime.datetime.now().date()
            if parse_result.year == date_now.year and parse_result.month > date_now.month:
                parse_result = parse_result.replace(year=parse_result.year - 1)
            if not self.perform_full and not self.last_depth:
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
    result['url'] = url
    result['html'] = "\n".join(response.css(self.text).extract())
    result['datetime_created'] = datetime.datetime.now().replace(tzinfo=pytz.timezone('Asia/Almaty'))
    # self.i += 1
    # print("!!!", self.i, response.meta['depth'])
    # print(result['datetime'], self.latest_date, self.last_depth)
    return result
