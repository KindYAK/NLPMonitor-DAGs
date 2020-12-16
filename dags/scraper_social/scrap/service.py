def scrap_wrapper(**kwargs):
    from django.utils import timezone
    from mainapp.models import Document, Source
    from scraping.models import SocialNetworkAccount

    from dags.scraper_social.scrap.utils import scrap_by_account, scrap_by_request
    from dags.scraper.scrap.service_monitoring import report_subscriptions

    parsing_start = timezone.now()
    by = kwargs['by']
    social_network = kwargs['social_network']
    source = dict(SocialNetworkAccount.SOCIAL_NETWORKS)[social_network]
    print("!", "Start parsing", by)
    if by == "account":
        accounts = kwargs['accounts']
        len_obj = len(accounts)
        fails, total = scrap_by_account(accounts, social_network)
    elif by == "request":
        requests = kwargs['requests']
        len_obj = len(requests)
        fails, total = scrap_by_request(requests, social_network)
    else:
        raise Exception("Not implemented")

    source = Source.objects.get(name=source)
    posts = Document.objects.filter(source=source, datetime_created__lte=parsing_start)
    posts = [
        {
            "text": post.text,
            "title": post.title,
            "datetime": post.datetime,
            "url": post.url,
        } for post in posts
    ]
    report_subscriptions(source, posts)

    if fails > len_obj // 2:
        raise Exception("Too many fails, WTF?")
    return f"Parse complete, {total} parsed, {fails} fails"
