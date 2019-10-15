from util.util import is_kazakh


def init_dictionary_index(**kwargs):
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_INDEX
    from mainapp.documents import Dictionary

    from util.service_es import search

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    number_of_documents = s.count()

    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_DICTIONARY_INDEX):
        query = {
            "corpus": kwargs['corpus'],
            "name": kwargs['name'],
            "number_of_documents": number_of_documents,
        }
        if search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, query):
            return ("!!!", "Already exists")

    kwargs["number_of_documents"] = number_of_documents
    kwargs["is_ready"] = False
    dictionary = Dictionary(**kwargs)
    dictionary.save()


def generate_dictionary_batch(**kwargs):
    import re
    from util.service_es import search, update_generator
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT
    from elasticsearch.helpers import parallel_bulk
    from stop_words import get_stop_words
    from pymorphy2 import MorphAnalyzer

    name = kwargs['name']
    start = kwargs['start']
    end = kwargs['end']

    query = {
        "name": name,
    }
    dictionary = search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, query)[-1]
    number_of_documents = dictionary['number_of_documents']
    if not number_of_documents:
        raise Exception("No variable!")

    s = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=['text'], sort=['id'], get_search_obj=True,
                       start=int(start/100*number_of_documents), end=int(end/100*number_of_documents)+1)

    stopwords = get_stop_words('ru')
    morph = MorphAnalyzer()
    dictionary_words = {}
    for doc in s.execute():
        if is_kazakh(doc.text + doc.title):
            continue
        word_in_doc = set()
        cleaned_words = (x for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі]|[^ ]*[*][^ ]*)', ' ', doc.text).split()).split())
        for word in cleaned_words:
            is_first_upper = word[0].isupper()
            word = word.lower()
            if word not in dictionary_words:
                parse = morph.parse(word)
                dictionary_words[word] = {
                    "dictionary": name,
                    "word": word,
                    "word_normal": parse[0].normal_form,
                    "is_in_pymorphy2_dict": parse[0].is_known,
                    "is_multiple_normals_in_pymorphy2": len(parse) > 1,
                    "is_stop_word": word in stopwords or parse[0].normal_form in stopwords,
                    "is_latin": any([c in "qwertyuiopasdfghjklzxcvbnm" for c in word]),
                    "is_kazakh": any([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in word]),
                    "pos_tag": parse[0].tag.POS,
                    "word_len": len(word),
                    "word_frequency": 1,
                    "document_frequency": 1,
                    "word_first_capital_ratio": 1 if is_first_upper else 0,
                }
            else:
                dictionary_words[word]['word_frequency'] += 1
                dictionary_words[word]['word_first_capital_ratio'] += 1 if word[0].isupper() else 0
                if word not in word_in_doc:
                    dictionary_words[word]['document_frequency'] += 1
            word_in_doc.add(word)

    for ok, result in parallel_bulk(ES_CLIENT, dictionary_words.values(), index=ES_INDEX_DICTIONARY_WORD + "_temp",
                                     chunk_size=10000, raise_on_error=True, thread_count=4):
        pass
    return s.count()


def aggregate_dicts(**kwargs):
    from util.service_es import search, update_generator
    from elasticsearch_dsl import Search
    from elasticsearch.helpers import parallel_bulk
    from nlpmonitor.settings import ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT

    name = kwargs['name']
    query = {
        "dictionary": name,
    }
    dictionary = search(ES_CLIENT, ES_INDEX_DICTIONARY_WORD + "_temp", query, get_search_obj=True)
    dictionary_index = search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, {"name": name})[-1]
    dictionary_words_final = {}
    dictionary_normal_words = {}
    for word in dictionary.scan():
        key = word['word']
        key_normal = word['word_normal']
        if not key in dictionary_words_final:
            dictionary_words_final[key] = word.to_dict()
        else:
            dictionary_words_final[key]['word_frequency'] += word['word_frequency']
            dictionary_words_final[key]['word_first_capital_ratio'] += word['word_first_capital_ratio']
            dictionary_words_final[key]['document_frequency'] += word['document_frequency']

        if not key_normal in dictionary_normal_words:
            dictionary_normal_words[key_normal] = {
                "word_normal_frequency": word['word_frequency'],
                "word_normal_first_capital_ratio": word['word_first_capital_ratio'],
                "document_normal_frequency": word['document_frequency']
            }
        else:
            dictionary_normal_words[key_normal]['word_normal_frequency'] += word['word_frequency']
            dictionary_normal_words[key_normal]['word_normal_first_capital_ratio'] += word['word_first_capital_ratio']
            dictionary_normal_words[key_normal]['document_normal_frequency'] += word['document_frequency']
    for key in dictionary_words_final.keys():
        dictionary_words_final[key]['word_normal_frequency'] = dictionary_normal_words[dictionary_words_final[key]['word_normal']]['word_normal_frequency']
        dictionary_words_final[key]['word_normal_first_capital_ratio'] = dictionary_normal_words[dictionary_words_final[key]['word_normal']]['word_normal_first_capital_ratio']
        dictionary_words_final[key]['document_normal_frequency'] = dictionary_normal_words[dictionary_words_final[key]['word_normal']]['document_normal_frequency']

        dictionary_words_final[key]['word_first_capital_ratio'] /= dictionary_words_final[key]['word_frequency']
        dictionary_words_final[key]['word_normal_first_capital_ratio'] /= dictionary_words_final[key]['word_normal_frequency']

        dictionary_words_final[key]['word_frequency_relative'] = dictionary_words_final[key]['word_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['word_normal_frequency_relative'] = dictionary_words_final[key]['word_normal_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['document_frequency_relative'] = dictionary_words_final[key]['document_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['document_normal_frequency_relative'] = dictionary_words_final[key]['document_normal_frequency'] / dictionary_index.number_of_documents

    for ok, result in parallel_bulk(ES_CLIENT, dictionary_words_final.values(),
                                     index=ES_INDEX_DICTIONARY_WORD,
                                     chunk_size=10000, raise_on_error=True, thread_count=4):
        pass

    ES_CLIENT.update(index=ES_INDEX_DICTIONARY_INDEX, id=dictionary_index.meta.id, body={"doc": {"is_ready": True}})
    return 0
