def es_update(**kwargs):
    import datetime

    from elasticsearch_dsl import Search
    from django.db.models import Q, F, ExpressionWrapper, fields
    from django.utils import timezone

    from mainapp.models import Document
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    # Init
    index = kwargs['index']
    now = timezone.now()
    print("!!!", "Getting documents to update", datetime.datetime.now())
    qs = Document.objects.exclude(
        id__in=(d.id for d in Document.objects.filter(num_views=None, num_comments=None))
    )
    qs = qs.only('id', 'num_views', 'num_comments', 'datetime_activity_parsed', 'datetime_activity_es_updated')
    qs = qs.annotate(
        timedelta_parsed_to_updated=ExpressionWrapper(
            F('datetime_activity_parsed') - F('datetime_activity_es_updated'),
            output_field=fields.DurationField()
        )
    )
    qs = qs.filter(timedelta_parsed_to_updated__gte=datetime.timedelta(minutes=1))
    number_of_documents = qs.count()
    if number_of_documents == 0:
        return "Nothing to update"

    print("!!!", "Start updating ES index", index, number_of_documents, "docs to update", datetime.datetime.now())
    updated = 0
    docs_processed = 0
    for doc in qs:
        update_body = {}
        if doc.num_views is not None:
            update_body['num_views'] = doc.num_views
        if doc.num_comments is not None:
            update_body['num_comments'] = doc.num_comments
        s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", id=str(doc.id))
        _ids = (hit.meta.id for hit in s.execute())
        for _id in _ids:
            s = Search(using=ES_CLIENT, index=index).filter("term", document_es_id=_id)
            _td_ids = (hit.meta.id for hit in s.execute())
            for _td_id in _td_ids:
                updated += 1
                ES_CLIENT.update(index=index,
                                 id=_td_id,
                                 body={"doc": update_body}
                                 )
        doc.datetime_activity_es_updated = now
        docs_processed += 1
        if updated != 0 and updated % 100000 == 0:
            print(f"{updated} updated")
        if docs_processed != 0 and docs_processed % 10000 == 0:
            print(f"{docs_processed}/{number_of_documents} processed", datetime.datetime.now())
    Document.objects.bulk_update(qs, fields=['datetime_activity_es_updated'])
    return f"{updated} docs updated"
