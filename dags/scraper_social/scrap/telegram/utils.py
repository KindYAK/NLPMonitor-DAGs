async def scrap_telegram_async(client, account, datetime_last=None):
    from dags.scraper_social.scrap.utils import create_document

    documents_parsed = 0
    async for message in client.iter_messages(account['account_id']):
        if not message or not message.text:
            continue
        if datetime_last and message.date < datetime_last:
            break
        result = create_document(
            source_name="Telegram",
            social_network_account_id=account['id'],
            title=f'Пост от {message.date}: {message.text[:50] + ("..." if len(message.text) > 50 else "")}',
            text=message.text,
            datetime=message.date,
            num_views=message.views,
            url=f"{account['id']}-{message.id}",
        )
        if result:
            documents_parsed += 1
    return documents_parsed
