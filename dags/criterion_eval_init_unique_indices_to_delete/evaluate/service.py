def init_uniques(**kwargs):
    import datetime

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, \
        ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS, ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.documents import DocumentEval, DocumentEvalUniqueIDs

    from elasticsearch_dsl import Search, Index
    from elasticsearch.helpers import parallel_bulk

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    topic_modelling = kwargs['topic_modelling']
    calc_virt_negative = 'calc_virt_negative' in kwargs
    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])

    # Create or update unique IDs index
    print("!!!", "Writing unique IDs", datetime.datetime.now())
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}", body={
                "settings": DocumentEvalUniqueIDs.Index.settings,
                "mappings": DocumentEvalUniqueIDs.Index.mappings
            }
        )

    def unique_ids_generator():
        s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}")
        indexed = set()
        for d in s.scan():
            if d.document_es_id in indexed:
                continue
            doc = DocumentEvalUniqueIDs()
            doc.document_es_id = d.document_es_id
            yield doc
            indexed.add(d.document_es_id)

    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in unique_ids_generator()),
                                    index=f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}",
                                    chunk_size=10000, raise_on_error=True, thread_count=6):
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1
        if (failed + success) % 10000 == 0:
            print(f"!!!{failed + success} processed", datetime.datetime.now())
    return success + failed
