def scrap_wrapper(**kwargs):
    from django.utils import timezone
    from scraping.models import SocialNetworkAccount

    accounts = kwargs['accounts']
    print("!", "Start parsing")
    total = 0
    fails = 0
    accounts = sorted(accounts, key=lambda x: x['priority_rate'], reverse=True)
    for i, account in enumerate(accounts, start=1):
        if i % (len(accounts) // 10 + 1) == 0:
            print("!!!", f"{i}/{len(accounts)} parsed")
        try:
            account_obj = SocialNetworkAccount.objects.get(id=account['id'])
        except SocialNetworkAccount.DoesNotExist as e:
            fails += 1
            print("!!! EXCEPTION getting account", e)
            continue
        now = timezone.now()
        social_network_id = kwargs['social_network']
        f, t = 0, 0
        if social_network_id == 0:
            # Parse Facebook
            raise Exception("Not implemented")
        if social_network_id == 1:
            f, t = scrap_vk(account_obj)
        if social_network_id == 2:
            # Parse Twitter
            raise Exception("Not implemented")
        if social_network_id == 3:
            f, t = scrap_instagram(account_obj)
        if social_network_id == 4:
            # Parse Telegram
            f, t = scrap_telegram(account_obj)
        if social_network_id == 5:
            # Parse Youtube
            raise Exception("Not implemented")
        fails += f
        total += t
        account_obj.datetime_last_parsed = now
        account_obj.save()

    if fails > len(list(accounts)) // 2:
        raise Exception("Too many fails, WTF?")
    return f"Parse complete, {total} parsed, {fails} fails"


def scrap_telegram(account):
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    from scraping.models import TelegramAuthKey, SocialNetworkAccount

    from dags.scraper_social.scrap.telegram.utils import scrap_telegram_async

    keys = TelegramAuthKey.objects.filter(is_active=True).order_by('?')

    fails = 0
    total = 0
    for key in keys:
        print("!!", "key", key.api_id)
        client = TelegramClient(StringSession(key.string_session), key.api_id, key.api_hash)
        nparsed = 0
        with client:
            try:
                nparsed = scrap_telegram_async(client, account, datetime_last=account.datetime_last_parsed)
            except ValueError as e:
                if "no user" in str(e).lower():
                    account.is_active = False
                    account.save()
                    print("Disabled user", account)
                    continue
                else:
                    print("!!! EXCEPTION ValueError", e)
                    fails += 1
                    continue
            except Exception as e:
                print("!!! EXCEPTION", e)
                fails += 1
                continue
            finally:
                total += nparsed
    return fails, total


def scrap_instagram(account):
    from dags.scraper_social.scrap.instagram.service import scrap_instagram_async

    fails = 0
    total = 0
    nparsed = 0
    try:
        nparsed = scrap_instagram_async(account, datetime_last=account.datetime_last_parsed)
    except Exception as e:
        print("!!! EXCEPTION", e)
        fails += 1
    finally:
        total += nparsed
    return fails, total


def scrap_vk(account):
    import vk

    from django.utils import timezone

    from scraping.models import VKLoginPass

    from dags.scraper_social.scrap.vk.utils import scrap_vk_async

    auth_accounts = VKLoginPass.objects.filter(is_active=True).order_by('?')

    fails = 0
    total = 0
    for key in auth_accounts:
        if key.datetime_wall_get_limit_reached and key.datetime_wall_get_limit_reached > timezone.now() - timezone.timedelta(days=1):
            print("!!! Skip blocked key", key.app_id)
            continue
        elif key.datetime_wall_get_limit_reached and key.datetime_wall_get_limit_reached >= timezone.now() - timezone.timedelta(days=1):
            key.wall_get_limit_used = 0
            key.save()

        if key.datetime_wall_get_updated and \
                key.datetime_wall_get_updated - key.datetime_wall_get_updated.date() > timezone.timedelta(days=1):
            key.wall_get_limit_used = 0
            key.auth_account.datetime_wall_get_updated = None
            key.save()

        print("!!", "key", key.app_id)
        session = vk.AuthSession(key.app_id, key.login, key.password)
        vk_api = vk.API(session)
        nparsed = 0
        try:
            nparsed = scrap_vk_async(vk_api, account, key, datetime_last=account.datetime_last_parsed)
        except Exception as e:
            print("!!! EXCEPTION", e)
            fails += 1
            continue
        finally:
            total += nparsed
    return fails, total
