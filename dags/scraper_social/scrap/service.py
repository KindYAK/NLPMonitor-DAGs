def scrap_wrapper(**kwargs):
    from dags.scraper_social.scrap.utils import scrap_by_account, scrap_by_request

    by = kwargs['by']
    social_network = kwargs['social_network']
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

    if fails > len_obj // 2:
        raise Exception("Too many fails, WTF?")
    return f"Parse complete, {total} parsed, {fails} fails"
