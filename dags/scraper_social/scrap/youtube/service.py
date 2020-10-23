import datetime
from django.utils import timezone

date_getter = lambda x: datetime.datetime.fromtimestamp(x['video_publish_date']).replace(tzinfo=timezone.utc)
text_getter = lambda x: x['video_description']


def scrap_youtube_async(yt_api, account, auth_account, datetime_last=None):
    import asyncio

    from dags.scraper_social.scrap.utils import scrap_wrapper_async

    async def iterator():
        from django.utils import timezone
        while True:
            try:
                posts = yt_api.search(channel_id=account.account_id, max_results=30, order_by='date')
            except Exception as e:
                print(e)
                auth_account.datetime_videos_limit_reached = timezone.now()
                auth_account.save()
                break
            for post in posts:
                yield post
            auth_account.videos_limit_used += 1
            if not auth_account.datetime_videos_updated:
                auth_account.datetime_videos_updated = timezone.now()
            auth_account.save()

    def document_handler(account, message):
        from dags.scraper_social.scrap.utils import create_document
        from dags.scraper_social.scrap.youtube.utils import parse_yt_comments

        video_meta = yt_api.get_video_metadata(message['video_id'])
        hashtags_list = video_meta['video_tags'].split('|')

        comments_list = list()
        if video_meta['video_comment_count']:
            comments = yt_api.get_video_comments(video_id=message['video_id'], max_results=20)  # list of dicts
            comments_list = parse_yt_comments(comments_dict_list=comments)  # comment_text, comment_date, comment_id

        return create_document(
            source_name="YouTube",
            social_network_account_id=account.id,
            title=f'Пост от {date_getter(message)}: {message["video_title"]}',
            text=video_meta['video_description'],
            datetime=date_getter(message),
            num_comments=video_meta['video_comment_count'],
            num_likes=video_meta['video_like_count'],
            num_views=video_meta['video_view_count'],
            url=f"{account.id}-{message['video_id']}",
            hashtags_list=hashtags_list,
            comments_list=comments_list
        )

    def document_updater(account, message):
        from mainapp.models import Document
        from dags.scraper_social.scrap.youtube.utils import parse_yt_comments
        from dags.scraper_social.scrap.utils import create_comments

        video_meta = yt_api.get_video_metadata(message['video_id'])
        try:
            d = Document.objects.get(url=f"{account.id}-{message['video_id']}")
        except Exception as e:
            print(f"{account.id}-{message['video_id']} not found!")
            raise e

        if video_meta['video_comment_count'] > d.num_comments:
            comments_to_add = video_meta['video_comment_count'] - d.num_comments
            comments = yt_api.get_video_comments(video_id=message['video_id'], max_results=comments_to_add)
            comments_list = parse_yt_comments(comments_dict_list=comments)
            create_comments(comments_list=comments_list, document=d)

        d.num_comments = video_meta['video_comment_count']
        d.num_likes = video_meta['video_like_count'],
        d.num_views = video_meta['video_view_count']
        d.save()

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(scrap_wrapper_async(account, iterator(), document_handler, document_updater,
                                                              date_getter, text_getter, datetime_last))
