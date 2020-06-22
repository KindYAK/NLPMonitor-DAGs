def init_last_datetime():
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).exclude('exists', field='text_lemmatized_kz_apertium')
    Variable.set("lemmatize_number_of_documents_kz", s.count())


def preprocessing_raw_data(**kwargs):
    import re
    import requests

    from airflow.models import Variable
    from elasticsearch.helpers import streaming_bulk
    from elasticsearch_dsl import Search, Q
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    from util.service_es import search, update_generator
    from util.util import is_word, is_kazakh

    start = kwargs['start']
    end = kwargs['end']

    number_of_documents = int(Variable.get("lemmatize_number_of_documents_kz", default_var=None))
    if number_of_documents is None:
        raise Exception("No variable!")

    s = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=['text'], sort=['id'], get_search_obj=True)
    s = s.query(~Q('exists', field="text_lemmatized_kz_apertium"))
    s = s[int(start / 100 * number_of_documents):int(end / 100 * number_of_documents) + 1]
    documents = s.execute()

    print('!!! len docs', len(documents))
    for doc in documents:
        if not is_kazakh(doc.text):
            continue
        cleaned_doc = " ".join(x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split())
        r = requests.get(f"http://apertium-flask:8005?text={cleaned_doc}")
        doc['text_lemmatized_kz_apertium'] = r.json()['result']

    documents_processed = 0
    failed = 0
    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=5000, raise_on_error=True, max_retries=10):
        if not ok:
            failed += 1
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        documents_processed += 1
    return f"{documents_processed} Processed"
