def init_uniques(**kwargs):
    import datetime
    import logging

    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Search
    from mainapp.documents import TopicDocument, TopicDocumentUniqueIDs
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_TOPIC_DOCUMENT, \
        ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    name = kwargs['name']

    # Create or update unique IDS index
    print("!!!", "Writing unique IDs", datetime.datetime.now())
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS}_{name}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS}_{name}", body={
                "settings": TopicDocumentUniqueIDs.Index.settings,
                "mappings": TopicDocumentUniqueIDs.Index.mappings
            }
        )

    def unique_ids_generator():
        s = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{name}").source(['document_es_id'])
        indexed = set()
        for d in s.scan():
            if d.document_es_id in indexed:
                continue
            doc = TopicDocumentUniqueIDs()
            doc.document_es_id = d.document_es_id
            yield doc
            indexed.add(d.document_es_id)

    success, failed = 0, 0
    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in unique_ids_generator()),
                                    index=f"{ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS}_{name}", chunk_size=50000, thread_count=6, raise_on_error=True):
        if ok:
            success += 1
        else:
            print("!!!", "ES index fail, error", result)
            failed += 1
        if failed > 3:
            raise Exception("Too many failed to ES!!")
        if (success + failed) % 50000 == 0:
            print(f'{success + failed}, {datetime.datetime.now()}')
    print("!!!", "Done writing", datetime.datetime.now())

    return success + failed
