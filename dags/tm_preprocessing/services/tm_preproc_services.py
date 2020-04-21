def init_last_datetime():
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).exclude('exists', field='text_lemmatized_yandex')
    Variable.set("lemmatize_number_of_documents", s.count())


known_counter = 0
custom_dict_counter = 0
not_in_dict_counter = 0
def morph_with_dictionary(morph, word, custom_dict):
    parse = morph.parse(word)[0]
    if word in custom_dict:
        global custom_dict_counter
        custom_dict_counter += 1
        return custom_dict[word]
    if parse.is_known:
        global known_counter
        known_counter += 1
        return parse.normal_form
    global not_in_dict_counter
    not_in_dict_counter += 1
    return ""


def preprocessing_raw_data(**kwargs):
    import re
    from util.service_es import search, update_generator
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_CUSTOM_DICTIONARY_WORD
    from elasticsearch.helpers import streaming_bulk
    from stop_words import get_stop_words
    from pymorphy2 import MorphAnalyzer
    from pymystem3 import Mystem
    from airflow.models import Variable
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import stopwords
    from util.util import is_latin

    start = kwargs['start']
    end = kwargs['end']

    number_of_documents = int(Variable.get("lemmatize_number_of_documents", default_var=None))
    if number_of_documents is None:
        raise Exception("No variable!")

    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=['text'], sort=['id'], get_scan_obj=True,
                       start=int(start/100*number_of_documents), end=int(end/100*number_of_documents)+1).exclude('exists', field="text_lemmatized_yandex")

    stopwords_ru = get_stop_words('ru')
    stopwords_eng = get_stop_words('en') + stopwords.words('english')

    lemmatizer = WordNetLemmatizer()
    morph = MorphAnalyzer()
    m = Mystem()

    s = Search(using=ES_CLIENT, index=ES_INDEX_CUSTOM_DICTIONARY_WORD)
    r = s[:1000000].scan()
    custom_dict = dict((w.word, w.word_normal) for w in r)

    for doc in documents:
        cleaned_doc = " ".join(x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split())
        if is_latin(cleaned_doc):
            cleaned_words_list = [lemmatizer.lemmatize(word) for word in cleaned_doc.split() if
                                  len(word) > 3 and word not in stopwords_eng]

        else:
            cleaned_words_list = [morph_with_dictionary(morph, word, custom_dict) for word in cleaned_doc.split() if
                                  len(word) > 2 and word not in stopwords_ru]
            cwl_yandex = filter(lambda word: len(word) > 2 and word not in stopwords_ru, m.lemmatize(cleaned_doc))
            cleaned_doc_yandex = " ".join(cwl_yandex)
            doc['text_lemmatized_yandex'] = cleaned_doc_yandex

        cleaned_doc = " ".join(cleaned_words_list)
        doc['text_lemmatized'] = cleaned_doc

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
    return f"{documents_processed} Processed, {known_counter} in pymorphie dict, {custom_dict_counter} in custom dict, {not_in_dict_counter} not found"
