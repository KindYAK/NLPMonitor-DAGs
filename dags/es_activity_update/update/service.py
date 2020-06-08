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
    from elasticsearch.helpers import parallel_bulk
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
    def update_generator():
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
                    yield {
                        "_index": index,
                        "_op_type": "update",
                        "_id": _td_id,
                        "doc": update_body,
                    }
            docs_processed += 1
            if docs_processed != 0 and docs_processed % 10000 == 0:
                print(f"{docs_processed}/{number_of_documents} processed", datetime.datetime.now())

    success = 0
    failed = 0
    for ok, result in parallel_bulk(ES_CLIENT, update_generator(),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=5000, raise_on_error=True, thread_count=4, max_retries=10):
        if not ok:
            failed += 1
        else:
            success += 1
        if success % 5000 == 0:
            print(f"{success} es docs updated, {datetime.datetime.now()}")
        if failed > 5:
            raise Exception("Too many failed ES!!!")
    return f"{success} docs updated"
