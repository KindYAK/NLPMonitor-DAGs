def scrap_instagram_async(account, datetime_last=None):
    from dags.scraper_social.scrap.utils import scrap_wrapper_async, create_comments
    from dags.scraper_social.scrap.instagram.utils import instagram_iterator, get_posts

    date_getter = lambda x: x.date
    text_getter = lambda x: x.caption
    iterator = instagram_iterator(account=account, batch_size=100)

    def document_handler(account, message):
        from dags.scraper_social.scrap.utils import create_document
        from datetime import datetime

        date = datetime.fromtimestamp(message.date)
        comments_duo = list()

        if message.comments_count:
            comments_duo = get_posts(method_type='Media', object_id=message, num_posts=100)  # nested list

        return create_document(
            source_name="Instagram",
            social_network_account_id=account.id,
            title=f'Пост от {str(date)}: {message.caption[:50] + ("..." if len(message.caption) > 50 else "")}',
            text=message.caption,
            datetime=date,
            num_likes=message.likes_count,
            num_comments=message.comments_count,
            url=message.display_url,
            comments_list=comments_duo
        )

    def document_updater(account, message):
        from mainapp.models import Document

        try:
            d = Document.objects.get(url=message.display_url)
        except Exception as e:
            print(f"{account.id}-{message.id} not found!")
            raise e
        d.num_likes = message.likes_count
        if message.comments_count > d.num_comments:
            comments_to_add = message.comments_count - d.num_comments
            comments_duo = get_posts(method_type='Media', object_id=message, num_posts=comments_to_add)
            create_comments(comments_list=comments_duo, document=d)

        # TODO add logic to compare num_comments before and now, and start parsing comments
        d.num_comments = message.comments_count
        d.save()

    return scrap_wrapper_async(account=account,
                               iterator=iterator,
                               document_handler=document_handler,
                               document_updater=document_updater,
                               date_getter=date_getter,
                               datetime_last=datetime_last,
                               text_getter=text_getter)
