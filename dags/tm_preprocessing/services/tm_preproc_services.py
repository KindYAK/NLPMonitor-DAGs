def init_last_datetime():
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).exclude('exists', field='text_lemmatized')
    Variable.set("lemmatize_number_of_documents", s.count())


def preprocessing_raw_data(**kwargs):
    import re
    from util.service_es import search, update_generator
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_INDEX_DOCUMENT, ES_CLIENT
    from elasticsearch.helpers import streaming_bulk
    from stop_words import get_stop_words
    from pymorphy2 import MorphAnalyzer
    from airflow.models import Variable

    start = kwargs['start']
    end = kwargs['end']

    number_of_documents = int(Variable.get("lemmatize_number_of_documents", default_var=None))
    if not number_of_documents:
        raise Exception("No variable!")

    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=['text'], sort=['id'], get_search_obj=True,
                       start=int(start/100*number_of_documents), end=int(end/100*number_of_documents)+1).exclude('exists', field="text_lemmatized")

    stopwords = get_stop_words('ru')
    morph = MorphAnalyzer()

    for doc in documents:
        cleaned_doc = " ".join(x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split())
        cleaned_doc = " ".join([morph.parse(word)[0].normal_form for word in cleaned_doc.split() if len(word) > 2 and word not in stopwords])
        doc['text_lemmatized'] = cleaned_doc

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=5000, raise_on_error=True, max_retries=10):
        print(ok, result)
    return len(documents)
