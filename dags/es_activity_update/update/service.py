def set_update_datetime():
    import datetime

    from airflow.models import Variable
    from django.db.models import F, ExpressionWrapper, fields

    from mainapp.models import Document

    update_datetime = Variable.get("es_activity_update_datetime")
    qs = Document.objects.exclude(
        id__in=(d.id for d in Document.objects.filter(num_views=None, num_comments=None))
    )
    qs = qs.only('id', 'datetime_activity_es_updated')
    qs = qs.annotate(
        timedelta_parsed_to_updated=ExpressionWrapper(
            F('datetime_activity_parsed') - F('datetime_activity_es_updated'),
            output_field=fields.DurationField()
        )
    )
    qs = qs.filter(timedelta_parsed_to_updated__gte=datetime.timedelta(minutes=1))
    for doc in qs:
        doc.datetime_activity_es_updated = update_datetime
    Document.objects.bulk_update(qs, fields=['datetime_activity_es_updated'])


def init_update_datetime():
    from django.utils import timezone
    from airflow.models import Variable

    Variable.set("es_activity_update_datetime", timezone.now().isoformat())


def es_update(**kwargs):
    import datetime

    from elasticsearch_dsl import Search
    from django.db.models import F, ExpressionWrapper, fields, Q

    from mainapp.models import Document
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    # Init
    index = kwargs['index']
    print("!!!", "Getting documents to update", datetime.datetime.now())
    qs = Document.objects.exclude(Q(num_views=None) & Q(num_comments=None))
    qs = qs.only('id', 'num_views', 'num_comments', 'datetime_activity_parsed', 'datetime_activity_es_updated')
    qs = qs.annotate(
        timedelta_parsed_to_updated=ExpressionWrapper(
            F('datetime_activity_parsed') - F('datetime_activity_es_updated'),
            output_field=fields.DurationField()
        )
    )
    qs = qs.filter(Q(timedelta_parsed_to_updated__gte=datetime.timedelta(minutes=1)) | Q(datetime_activity_es_updated=None))
    number_of_documents = qs.count()
    if number_of_documents == 0:
        return "Nothing to update"

    print("!!!", "Start updating ES index", number_of_documents, "docs to update", datetime.datetime.now())
    updated = 0
    docs_processed = 0
    for doc in qs:
        update_body = {}
        if doc.num_views is not None:
            update_body['num_views'] = doc.num_views
        if doc.num_comments is not None:
            update_body['num_comments'] = doc.num_comments
        s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", id=doc.id)[:100]
        _ids = (hit.meta.id for hit in s.execute())
        for _id in _ids:
            s = Search(using=ES_CLIENT, index=index).filter("term", document_es_id=_id)[:100]
            _td_ids = (hit.meta.id for hit in s.execute())
            for _td_id in _td_ids:
                updated += 1
                ES_CLIENT.update(index=index,
                                 id=_td_id,
                                 body={"doc": update_body}
                                 )
                if updated != 0 and updated % 10000 == 0:
                    print(f"{updated} updated")
        docs_processed += 1
        if docs_processed != 0 and docs_processed % 10000 == 0:
            print(f"{docs_processed}/{number_of_documents} processed", datetime.datetime.now())
    return f"{updated} docs updated"
