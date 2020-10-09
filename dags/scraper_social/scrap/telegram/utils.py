def scrap_telegram_async(client, account, datetime_last=None):
    from dags.scraper_social.scrap.utils import scrap_wrapper_async

    iterator = client.iter_messages(account.account_id)
    date_getter = lambda x: x.date
    text_getter = lambda x: x.text

    def document_handler(account, message):
        from dags.scraper_social.scrap.utils import create_document

        return create_document(
            source_name="Telegram",
            social_network_account_id=account.id,
            title=f'Пост от {message.date}: {message.text[:50] + ("..." if len(message.text) > 50 else "")}',
            text=message.text,
            datetime=message.date,
            num_views=message.views,
            url=f"{account.id}-{message.id}",
        )

    def document_updater(account, message):
        from mainapp.models import Document

        try:
            d = Document.objects.get(url=f"{account.id}-{message.id}")
        except Exception as e:
            print(f"{account.id}-{message.id} not found!")
            raise e
        d.num_views = message.views
        d.save()

    return client.loop.run_until_complete(scrap_wrapper_async(account, iterator, document_handler, document_updater,
                                                              date_getter, text_getter, datetime_last))
