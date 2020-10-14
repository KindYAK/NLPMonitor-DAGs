def scrap_by_account(accounts, social_network):
    from django.utils import timezone
    from scraping.models import SocialNetworkAccount

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
            print("!!! EXCEPTION getting scraping_obj", e)
            continue
        now = timezone.now()
        social_network_id = social_network
        f, t = 0, 0
        if social_network_id == 0:
            # Parse Facebook
            raise Exception("Not implemented")
        if social_network_id == 1:
            # Parse VK
            from dags.scraper_social.scrap.vk.utils import scrap_vk_async
            f, t = scrap_vk(account_obj, scrap_vk_async, "wall")
        if social_network_id == 2:
            # Parse Twitter
            raise Exception("Not implemented")
        if social_network_id == 3:
            # Parse Instagram
            f, t = scrap_instagram(account_obj)
        if social_network_id == 4:
            # Parse Telegram
            f, t = scrap_telegram(account_obj)
        if social_network_id == 5:
            from dags.scraper_social.scrap.youtube.service import scrap_youtube_async
            f, t = scrap_youtube(account_obj, scrap_youtube_async)
        fails += f
        total += t
        account_obj.datetime_last_parsed = now
        account_obj.save()
    return fails, total


def scrap_by_request(requests, social_network):
    from django.utils import timezone
    from scraping.models import MonitoringQuery

    total = 0
    fails = 0
    requests = sorted(requests, key=lambda x: x['priority_rate'], reverse=True)
    for i, request in enumerate(requests, start=1):
        if i % (len(requests) // 10 + 1) == 0:
            print("!!!", f"{i}/{len(requests)} parsed")
        try:
            request_object = MonitoringQuery.objects.get(id=request['id'])
        except MonitoringQuery.DoesNotExist as e:
            fails += 1
            print("!!! EXCEPTION getting scraping_obj", e)
            continue
        now = timezone.now()
        social_network_id = social_network
        f, t = 0, 0
        if social_network_id == 0:
            # Parse Facebook
            raise Exception("Not implemented")
        if social_network_id == 1:
            # Parse VK
            from dags.scraper_social.scrap.vk.utils import scrap_vk_async_by_request
            f, t = scrap_vk(request_object, scrap_vk_async_by_request, "feed")
        if social_network_id == 2:
            # Parse Twitter
            raise Exception("Not implemented")
        if social_network_id == 3:
            # Parse Instagram
            raise Exception("Not implemented")
        if social_network_id == 4:
            # Parse Telegram
            raise Exception("Not implemented")
        if social_network_id == 5:
            # Parse Youtube
            raise Exception("Not implemented")
        fails += f
        total += t
        request_object.datetime_last_parsed = now
        request_object.save()
    return fails, total


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


def scrap_vk(scraping_obj, scrap_function, rtype):
    import vk

    from django.utils import timezone

    from scraping.models import VKLoginPass

    auth_accounts = VKLoginPass.objects.filter(is_active=True).order_by('?')

    fails = 0
    total = 0

    for key in auth_accounts:

        if rtype == "wall":
            if key.datetime_wall_get_limit_reached and ((key.datetime_wall_get_limit_reached > (timezone.now()) - timezone.timedelta(days=1))):
                print("!!! Skip blocked key", key.app_id)
                continue
            elif key.datetime_wall_get_limit_reached and (key.datetime_wall_get_limit_reached <= (timezone.now() - timezone.timedelta(days=1))):
                key.wall_get_limit_used = 0
                key.save()
        elif rtype == "feed":
            if key.datetime_wall_get_limit_reached and ((key.datetime_wall_get_limit_reached > (timezone.now()) - timezone.timedelta(days=1))):
                print("!!! Skip blocked key", key.app_id)
                continue
            elif key.datetime_wall_get_limit_reached and (key.datetime_wall_get_limit_reached <= (timezone.now() - timezone.timedelta(days=1))):
                key.news_feed_limit_used = 0
                key.save()
        else:
            raise Exception("Not implemented (rtype)!")

        if key.datetime_wall_get_updated and ((key.datetime_wall_get_updated.date() - key.datetime_wall_get_updated.date()) > timezone.timedelta(days=1)):
            key.wall_get_limit_used = 0
            key.auth_account.datetime_wall_get_updated = None
            key.save()

        print("!!", "key", key.app_id)
        session = vk.AuthSession(key.app_id, key.login, key.password)
        vk_api = vk.API(session)
        nparsed = 0
        try:
            nparsed = scrap_function(vk_api, scraping_obj, key, datetime_last=scraping_obj.datetime_last_parsed)
        except Exception as e:
            print("!!! EXCEPTION", e)
            fails += 1
            continue
        finally:
            total += nparsed
    return fails, total


def scrap_youtube(scraping_obj, scrap_function):
    from django.utils import timezone
    from scraping.models import YouTubeAuthToken
    from youtube_api import YouTubeDataAPI

    auth_accounts = YouTubeAuthToken.objects.filter(is_active=True).order_by('?')

    fails = 0
    total = 0

    for key in auth_accounts:
        if key.datetime_videos_limit_reached and ((key.datetime_videos_limit_reached > (timezone.now()) - timezone.timedelta(days=1))):
            print("!!! Skip blocked key", key.token_id)
            continue
        elif key.datetime_videos_limit_reached and (key.datetime_videos_limit_reached <= (timezone.now() - timezone.timedelta(days=1))):
            key.videos_limit_used = 0
            key.save()
        if key.datetime_videos_updated and ((key.datetime_videos_updated.date() - key.datetime_videos_updated.date()) > timezone.timedelta(days=1)):
            key.videos_limit_used = 0
            key.auth_account.datetime_videos_updated = None
            key.save()

        print("!!", "key", key.token_id)

        yt_api = YouTubeDataAPI(key.token_id)
        nparsed = 0
        try:
            nparsed = scrap_function(yt_api, scraping_obj, key, datetime_last=scraping_obj.datetime_last_parsed)
        except Exception as e:
            print("!!! EXCEPTION", e)
            fails += 1
            continue
        finally:
            total += nparsed
    return fails, total


def create_document(source_name, title, text,  # Required stuff
                    # Optional stuff
                    html=None, url=None, datetime=None, hashtags_list=None,
                    num_views=None, num_shares=None, num_comments=None, num_likes=None, comments_list=None,
                    # If you know DB ID of Social Account
                    social_network_account_id=None,
                    # If you don't know DB ID of Social Account
                    social_network_choice_int=None,
                    social_network_account_name=None, social_network_account_internal_id=None,
                    social_network_account_nickname=None, social_network_account_url=None):
    from annoying.functions import get_object_or_None

    from mainapp.models import Corpus, Source, Document, Tag, Comment
    from scraping.models import SocialNetworkAccount

    corpus = get_object_or_None(Corpus, name="main")
    if not corpus:
        corpus = Source.objects.create(name=source_name, url=source_name, corpus="main")

    source = get_object_or_None(Source, name=source_name, corpus=corpus)
    if not source:
        source = Source.objects.create(name=source_name, url=source_name, corpus=corpus)

    if hashtags_list:
        for hashtag in hashtags_list:
            tag = get_object_or_None(Tag, name=hashtag, corpus=corpus)
            if not tag:
                Tag.objects.create(name=hashtag, corpus=corpus)

    account = None
    if social_network_account_id:
        account = SocialNetworkAccount.objects.get(id=social_network_account_id)
    elif social_network_account_name and social_network_account_internal_id:
        account = get_object_or_None(SocialNetworkAccount, social_network=social_network_choice_int,
                                     account_id=social_network_account_internal_id)
        if not account:
            account = SocialNetworkAccount.objects.create(name=social_network_account_name,
                                                          social_network=social_network_choice_int,
                                                          url=social_network_account_url,
                                                          account_id=social_network_account_internal_id,
                                                          nickname=social_network_account_nickname)

    try:
        document = Document.objects.create(
            source=source,
            social_network_account=account,
            title=title,
            text=text,
            html=html,
            url=url,
            datetime=datetime,
            num_views=num_views,
            num_likes=num_likes,
            num_shares=num_shares,
            num_comments=num_comments,
        )

        create_comments(comments_list=comments_list, document=document)

        return True
    except Exception as e:
        if "duplicate" not in str(e).lower():
            raise e
        else:
            return False


async def scrap_wrapper_async(scraping_object, iterator, document_handler, document_updater, date_getter, text_getter,
                              datetime_last=None):
    import datetime

    from mainapp.models import Document

    documents_parsed = 0
    update_mode = False
    async for message in iterator:
        if not message or not text_getter(message):
            continue
        if datetime_last and date_getter(message) < datetime_last:
            update_mode = True
        if datetime_last and date_getter(message) < (datetime_last - datetime.timedelta(days=3)):
            break
        if not update_mode:
            result = document_handler(scraping_object, message)
            if result:
                documents_parsed += 1
        else:
            try:
                document_updater(scraping_object, message)
            except Document.DoesNotExist as e:
                document_handler(scraping_object, message)
    return documents_parsed


def create_comments(comments_list, document):
    from mainapp.models import Comment
    from annoying.functions import get_object_or_None

    if not comments_list:
        return None

    for comment_text, comment_date, comment_id in comments_list:
        comment_object = get_object_or_None(Comment, comment_id=comment_id, document=document)
        if not comment_object:
            try:
                Comment.objects.create(text=comment_text, document=document, datetime=parse_date(comment_date),
                                       comment_id=comment_id)
            except Exception as e:
                if "duplicate" not in str(e).lower():
                    raise e
                else:
                    return False


def parse_date(datetime_object):
    from datetime import datetime as date_time
    from datetime import timezone
    from datetime import timedelta

    dt = date_time.fromtimestamp(datetime_object)
    dt = dt.replace(tzinfo=timezone.utc) - timedelta(hours=6)
    return dt
