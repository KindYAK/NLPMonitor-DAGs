async def scrap_wrapper_async(account, iterator, document_handler, document_updater, date_getter, datetime_last=None):
    import datetime

    documents_parsed = 0
    update_mode = False
    async for message in iterator:
        if not message or not message.text:
            continue
        if datetime_last and date_getter(message) < datetime_last:
            update_mode = True
        if datetime_last and date_getter(message) < (datetime_last - datetime.timedelta(days=3)):
            break
        if not update_mode:
            result = document_handler(account, message)
            if result:
                documents_parsed += 1
        else:
            document_updater(account, message)
    return documents_parsed


def scrap_telegram_async(client, account, datetime_last=None):
    iterator = client.iter_messages(account['account_id'])
    date_getter = lambda x: x.date

    def document_handler(account, message):
        from dags.scraper_social.scrap.utils import create_document

        return create_document(
            source_name="Telegram",
            social_network_account_id=account['id'],
            title=f'Пост от {message.date}: {message.text[:50] + ("..." if len(message.text) > 50 else "")}',
            text=message.text,
            datetime=message.date,
            num_views=message.views,
            url=f"{account['id']}-{message.id}",
        )

    def document_updater(account, message):
        from mainapp.models import Corpus, Source, Document

        try:
            d = Document.objects.get(url=f"{account['id']}-{message.id}")
        except Exception as e:
            print(f"{account['id']}-{message.id} not found!")
            return
        d.num_views = message.views
        d.save()

    return client.loop.run_until_complete(scrap_wrapper_async(account, iterator, document_handler, document_updater, date_getter, datetime_last))
