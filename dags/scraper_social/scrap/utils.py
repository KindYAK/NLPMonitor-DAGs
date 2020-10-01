def create_document(source_name, title, text, # Required stuff
                    # Optional stuff
                    html=None, url=None, datetime=None,
                    num_views=None, num_shares=None, num_comments=None, num_likes=None,
                    # If you know DB ID of Social Account
                    social_network_account_id=None,
                    # If you don't know DB ID of Social Account
                    social_network_choice_int=None,
                    social_network_account_name=None, social_network_account_internal_id=None,
                    social_network_account_nickname=None, social_network_account_url=None):
    from annoying.functions import get_object_or_None

    from mainapp.models import Corpus, Source, Document
    from scraping.models import SocialNetworkAccount

    corpus = get_object_or_None(Corpus, name="main")
    if not corpus:
        corpus = Source.objects.create(name=source_name, url=source_name, corpus="main")

    source = get_object_or_None(Source, name=source_name, corpus=corpus)
    if not source:
        source = Source.objects.create(name=source_name, url=source_name, corpus=corpus)

    if social_network_account_id:
        account = SocialNetworkAccount.objects.get(id=social_network_account_id)
    else:
        account = get_object_or_None(SocialNetworkAccount, social_network=social_network_choice_int, account_id=social_network_account_internal_id)
        if not account:
            account = SocialNetworkAccount.objects.create(name=social_network_account_name,
                                                          social_network=social_network_choice_int,
                                                          url=social_network_account_url,
                                                          account_id=social_network_account_internal_id,
                                                          nickname=social_network_account_nickname)
    try:
        Document.objects.create(
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
        return True
    except Exception as e:
        if not "duplicate" in str(e).lower():
            raise e
        else:
            return False


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
