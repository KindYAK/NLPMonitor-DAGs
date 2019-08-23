def es_etl(**kwargs):
    from util.service_es import search, update_generator
    from util.constants import BASE_DAG_DIR

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    stuff = kwargs['stuff']

    # Extract
    query = {
        "corpus": "main",

    }
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query)
    print("!!!", len(documents))

    # Transform
    for document in documents:
        if 'num_views' in document:
            document.num_views += 1
        document.any_stuff = stuff
        document.literally_any_stuff = {
            "literally": [{"any_stuff": [1, 2, 3, 4, 5, 6]}]
        }
    print("!!!", list(documents[0].to_dict().keys()))
    print("!!!", documents[0].any_stuff)
    print("!!!", documents[0].literally_any_stuff)

    # Load
    from elasticsearch.helpers import streaming_bulk

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents), index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        print(ok, result)
