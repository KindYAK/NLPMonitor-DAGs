import datetime
from django.utils import timezone

date_getter = lambda x: datetime.datetime.fromtimestamp(x['date']).replace(tzinfo=timezone.utc)
text_getter = lambda x: x['text']


def scrap_vk_async(vk_api, account, auth_account, datetime_last=None):
    import asyncio

    from dags.scraper_social.scrap.utils import scrap_wrapper_async

    from scraping.models import VKLoginPass

    async def iterator():
        from django.utils import timezone

        offset = 0
        while True:
            try:
                posts = vk_api.wall.get(owner_id=f"-{account.account_id}", offset=offset, count=VKLoginPass.WALL_GET_MAX_COUNT, v=VKLoginPass.API_V)
            except Exception as e:
                if not "29" in str(e):
                    raise e
                auth_account.datetime_wall_get_limit_reached = timezone.now()
                auth_account.save()
                break
            if 'items' not in posts and not posts['items']:
                break
            for post in posts['items']:
                yield post
            offset += VKLoginPass.WALL_GET_MAX_COUNT
            auth_account.wall_get_limit_used += 1
            if not auth_account.datetime_wall_get_updated:
                auth_account.datetime_wall_get_updated = timezone.now()
            auth_account.save()

    def document_handler(account, message):
        from dags.scraper_social.scrap.utils import create_document

        return create_document(
            source_name="VK",
            social_network_account_id=account.id,
            title=f'Пост от {date_getter(message)}: {message["text"][:50] + ("..." if len(message["text"]) > 50 else "")}',
            text=message["text"],
            datetime=date_getter(message),
            num_comments=message['comments']['count'],
            num_shares=message['reposts']['count'],
            num_likes=message['likes']['count'],
            url=f"{account.id}-{message['id']}",
        )

    def document_updater(account, message):
        from mainapp.models import Document

        try:
            d = Document.objects.get(url=f"{account.id}-{message['id']}")
        except Exception as e:
            print(f"{account.id}-{message['id']} not found!")
            raise e
        d.num_comments = message['comments']['count']
        d.num_likes = message['likes']['count']
        d.num_shares = message['reposts']['count']
        d.save()

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(scrap_wrapper_async(account, iterator(), document_handler, document_updater,
                                                              date_getter, text_getter, datetime_last))


def scrap_vk_async_by_request(vk_api, request, auth_account, datetime_last=None):
    import asyncio

    from dags.scraper_social.scrap.utils import scrap_wrapper_async

    from scraping.models import VKLoginPass

    async def iterator():
        from django.utils import timezone

        start_from = None
        requests_done = 0
        while True:
            if requests_done > request.max_requests_per_session:
                break
            try:
                posts = vk_api.newsfeed.search(q=request.query, start_from=start_from, count=VKLoginPass.NEWS_FEED_MAX_COUNT, v=VKLoginPass.API_V)
            except Exception as e:
                if not "29" in str(e):
                    raise e
                auth_account.datetime_news_feed_limit_reached = timezone.now()
                auth_account.save()
                break
            if 'items' not in posts and not posts['items']:
                break
            for post in posts['items']:
                yield post
            start_from += posts['next_from']
            auth_account.wall_get_limit_used += 1
            requests_done += 1
            if not auth_account.datetime_news_feed_updated:
                auth_account.datetime_news_feed_updated = timezone.now()
            auth_account.save()

    def document_handler(request, message):
        from dags.scraper_social.scrap.utils import create_document

        return create_document(
            source_name="VK",
            title=f'Пост от {date_getter(message)}: {message["text"][:50] + ("..." if len(message["text"]) > 50 else "")}',
            text=message["text"],
            datetime=date_getter(message),
            num_comments=message['comments']['count'],
            num_shares=message['reposts']['count'],
            num_likes=message['likes']['count'],
            url=f"{message['owner_id']}-{message['id']}",
        )

    def document_updater(request, message):
        from mainapp.models import Document

        try:
            d = Document.objects.get(url=f"{message['owner_id']}-{message['id']}")
        except Exception as e:
            print(f"{message['owner_id']}-{message['id']} not found!")
            raise e
        d.num_comments = message['comments']['count']
        d.num_likes = message['likes']['count']
        d.num_shares = message['reposts']['count']
        d.save()

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(scrap_wrapper_async(request, iterator(), document_handler, document_updater,
                                                              date_getter, text_getter, datetime_last))