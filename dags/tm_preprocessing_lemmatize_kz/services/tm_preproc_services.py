def init_last_datetime():
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    s = s.exclude('exists', field="is_kazakh")
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
    s = s.exclude('exists', field="is_kazakh")
    s = s[int(start / 100 * number_of_documents):int(end / 100 * number_of_documents) + 1]
    documents = s.execute()

    print('!!! len docs', len(documents))
    for doc in documents:
        if not is_kazakh(doc.text):
            doc['is_kazakh'] = False
            continue
        cleaned_doc = [x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split()]
        result = ""
        for i in range(len(cleaned_doc) // 10000 + 1):
            req_text = ' '.join(cleaned_doc[i*10000:(i+1)*10000])
            r = requests.get(f"http://apertium-flask:8005?text={req_text}")
            result += r.json()['result']
        doc['text_lemmatized_kz_apertium'] = result
        doc['is_kazakh'] = True

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
