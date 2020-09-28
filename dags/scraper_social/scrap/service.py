def scrap_wrapper(**kwargs):
    social_network_id = kwargs['social_network']
    result = None
    if social_network_id == 0:
        # Parse Facebook
        raise Exception("Not implemented")
    if social_network_id == 1:
        # Parse VK
        raise Exception("Not implemented")
    if social_network_id == 2:
        # Parse Twitter
        raise Exception("Not implemented")
    if social_network_id == 3:
        # Parse Instagram
        raise Exception("Not implemented")
    if social_network_id == 4:
        # Parse Telegram
        result = scrap_telegram(**kwargs)
        print("!!", result)
    if social_network_id == 5:
        # Parse Youtube
        raise Exception("Not implemented")
    if result is None:
        raise Exception("Not implemented")
    else:
        return result


def scrap_telegram(**kwargs):
    from django.utils import timezone

    from telethon import TelegramClient
    from telethon.sessions import StringSession

    from scraping.models import TelegramAuthKey, SocialNetworkAccount

    from dags.scraper_social.scrap.telegram.utils import scrap_telegram_async

    keys = TelegramAuthKey.objects.filter(is_active=True).order_by('?')
    accounts = kwargs['accounts']

    print("!", "Start parsing")
    total = 0
    fails = 0
    for i, account in enumerate(accounts):
        if i % (len(accounts) // 10 + 1) == 0:
            print("!!!", f"{i}/{len(accounts)} parsed")
        account_obj = SocialNetworkAccount.objects.get(id=account['id'])
        now = timezone.now()
        for key in keys:
            print("!!", "key", key.api_id)
            client = TelegramClient(StringSession(key.string_session), key.api_id, key.api_hash)
            with client:
                nparsed = client.loop.run_until_complete(
                    scrap_telegram_async(client, account, datetime_last=account_obj.datetime_last_parsed))
                # try:
                #     nparsed = client.loop.run_until_complete(scrap_telegram_async(client, account, datetime_last=account_obj.datetime_last_parsed))
                # except Exception as e:
                #     print("!!! EXCEPTION", e)
                #     fails += 1
                #     continue
                # finally:
                #     total += nparsed
            account_obj.datetime_last_parsed = now
            account_obj.save()

    if fails > len(list(accounts)) // 2:
        raise Exception("Too many fails, WTF?")
    return f"Parse complete, {total} parsed"
