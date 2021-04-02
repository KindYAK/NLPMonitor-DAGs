def init_last_datetime(**kwargs):
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    s = s.exclude('exists', field="is_english")
    Variable.set("lemmatize_number_of_documents_eng", s.count())


def preprocessing_raw_data(**kwargs):
    import re

    from airflow.models import Variable
    from elasticsearch.helpers import streaming_bulk
    from lemminflect import getAllLemmas, getAllLemmasOOV
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
    from nltk.corpus import stopwords
    from stop_words import get_stop_words
    from util.service_es import search, update_generator
    from util.util import is_latin

    start = kwargs['start']
    end = kwargs['end']

    number_of_documents = int(Variable.get("lemmatize_number_of_documents_eng", default_var=None))
    if number_of_documents is None:
        raise Exception("No variable!")

    s = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=['text'], sort=['id'], get_search_obj=True)
    s = s.exclude('exists', field="is_english")
    start = int(start / 100 * number_of_documents)
    end = int(end / 100 * number_of_documents) + 1
    if end - start < 10_000:
        s = s[start:end]
    else:
        s = s[start:start+10_000]
    documents = s.execute()

    stopwords = set(get_stop_words('ru') + get_stop_words('en') + stopwords.words('english'))
    print('!!! len docs', len(documents))
    for doc in documents:
        if not is_latin(doc.text):
            doc['is_english'] = False
            continue
        cleaned_doc = [x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split() if not x in stopwords and len(x) > 2]
        result = ""
        for word in cleaned_doc:
            try:
                result += list(getAllLemmas(word).values())[0][0] + " "
            except IndexError:
                result += list(getAllLemmasOOV(word, upos="NOUN").values())[0][0] + " "
        doc['text_lemmatized_eng_lemminflect'] = result
        doc['is_english'] = True

    documents_processed = 0
    failed = 0
    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        if not ok:
            failed += 1
        if failed > 10:
            raise Exception("Too many failed ES!!!")
        documents_processed += 1
    return f"{documents_processed} Processed"
