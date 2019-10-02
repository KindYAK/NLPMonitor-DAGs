def preprocessing_raw_data():

    from util.service_es import search, get_count, update_generator
    from nlpmonitor.settings import ES_INDEX_DOCUMENT, ES_CLIENT
    from elasticsearch.helpers import streaming_bulk
    from stop_words import get_stop_words
    from pymorphy2 import MorphAnalyzer
    import re

    document_len = get_count(ES_CLIENT, ES_INDEX_DOCUMENT)
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, start=0, end=document_len, source=['text'])
    stopwords = get_stop_words('ru')
    morph = MorphAnalyzer()

    for doc in documents:

        cleaned_doc = " ".join(
            x.lower() for x in ' '.join(re.sub('([^А-Яа-я0-9 ]|[^ ]*[0-9][^ ]*)', ' ', doc.text).split()).split())

        cleaned_doc = [morph.parse(word)[0].normal_form for word in cleaned_doc.split() if
                       len(word) > 2 and word not in stopwords]

        doc['lemmatized_words'] = cleaned_doc

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        print(ok, result)
