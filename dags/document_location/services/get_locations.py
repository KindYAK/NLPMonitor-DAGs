def get_locations(**kwargs):
    from elasticsearch.helpers import parallel_bulk
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT_LOCATION
    from.util import locations_generator

    import datetime

    failed = 0
    success = 0

    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in locations_generator(**kwargs)),
                                    index=ES_INDEX_DOCUMENT_LOCATION,
                                    chunk_size=10000, raise_on_error=True, thread_count=4):
        if (failed + success) % 10000 == 0:
            print(f"!!!{failed + success} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1

    return 'Done'
