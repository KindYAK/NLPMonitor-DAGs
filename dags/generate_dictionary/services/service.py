def init_dictionary_index(**kwargs):
    from elasticsearch_dsl import Search, Index

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD
    from mainapp.documents import Dictionary, DictionaryWord

    from util.service_es import search

    name = kwargs['name']
    es_index = Index(f"{ES_INDEX_DICTIONARY_WORD}_{name}", using=ES_CLIENT)
    es_index.delete(ignore=404)
    settings = DictionaryWord.Index.settings
    ES_CLIENT.indices.create(
        index=f"{ES_INDEX_DICTIONARY_WORD}_{name}",
        body={
            "settings": settings,
            "mappings": DictionaryWord.Index.mappings
        }
    )

    es_index = Index(f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp", using=ES_CLIENT)
    es_index.delete(ignore=404)
    settings = DictionaryWord.Index.settings
    ES_CLIENT.indices.create(
        index=f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp",
        body={
            "settings": settings,
            "mappings": DictionaryWord.Index.mappings
        }
    )

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("terms", corpus=kwargs['corpuses'])
    number_of_documents = s.count()

    kwargs['corpuses'] = ",".join(kwargs['corpuses'])
    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_DICTIONARY_INDEX):
        query = {
            "corpus": kwargs['corpuses'],
            "name": kwargs['name']
        }
        if search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, query):
            return "Already exists"

    kwargs["number_of_documents"] = number_of_documents
    kwargs["is_ready"] = False
    dictionary = Dictionary(**kwargs)
    dictionary.save()
    return "Created"


def generate_dictionary_batch(**kwargs):
    import datetime
    import re

    from elasticsearch.helpers import parallel_bulk
    from pymorphy2 import MorphAnalyzer
    from stop_words import get_stop_words

    from util.util import is_kazakh, is_latin
    from util.service_es import search

    from nlpmonitor.settings import ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT

    name = kwargs['name']
    start = kwargs['start']
    end = kwargs['end']
    corpuses = kwargs['corpuses']
    max_n_gram_len = kwargs['max_n_gram_len']
    min_relative_document_frequency = kwargs['min_relative_document_frequency']
    field_to_parse = kwargs['field_to_parse']

    query = {
        "name": name,
        "is_ready": False,
    }
    dictionary = search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, query)[-1]
    number_of_documents = dictionary.number_of_documents
    if not number_of_documents:
        raise Exception("No variable!")

    print("!!!", "Getting documents from ES", datetime.datetime.now())
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT,
                       query={"corpus": corpuses},
                       source=[field_to_parse],
                       sort=['id'],
                       start=int(start / 100 * number_of_documents),
                       end=int(end / 100 * number_of_documents) + 1,
                       get_search_obj=True,
                       )
    documents = documents.filter("exists", field=field_to_parse).execute()

    stopwords = get_stop_words('ru')
    morph = MorphAnalyzer()
    dictionary_words = {}
    print("!!!", "Iterating through documents", datetime.datetime.now())
    for doc in documents:
        if len(doc[field_to_parse]) == 0:
            print("!!! WTF", doc.meta.id)
            continue
        if is_kazakh(doc[field_to_parse]) or is_latin(doc[field_to_parse]):
            continue
        word_in_doc = set()
        cleaned_words = [x for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc[field_to_parse]).split()).split()]
        for n_gram_len in range(1, max_n_gram_len + 1):
            for n_gram in (cleaned_words[i:i + n_gram_len] for i in range(len(cleaned_words) - n_gram_len + 1)):
                word = "_".join(n_gram)
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
                        "is_latin": any([c in "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM" for c in word]),
                        "is_kazakh": any([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in word]),
                        "n_gram_len": n_gram_len,
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
    len_dictionary = len(dictionary_words)
    dictionary_words = filter(lambda x: x['document_frequency'] > len(documents) * min_relative_document_frequency, dictionary_words.values())
    success = 0
    failed = 0
    print("!!!", "Writing to ES", datetime.datetime.now())
    for ok, result in parallel_bulk(ES_CLIENT, dictionary_words, index=f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp",
                                    chunk_size=10000, raise_on_error=True, thread_count=6):
        if not ok:
            failed += 1
        else:
            success += 1
        if success % 10000 == 0:
            print(f"{success}/{len_dictionary} processed, {datetime.datetime.now()}")
        if failed > 3:
            raise Exception("Too many failed!!")
    return len(documents)


def aggregate_dicts(**kwargs):
    import datetime

    from util.service_es import search
    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Search, Index
    from nlpmonitor.settings import ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT, ES_INDEX_DOCUMENT

    name = kwargs['name']
    corpuses = kwargs['corpuses']
    min_relative_document_frequency = kwargs['min_relative_document_frequency']
    query = {
        "dictionary": name,
    }
    dictionary_scan = search(ES_CLIENT, f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp" , query, get_scan_obj=True)
    dictionary_index = search(ES_CLIENT, ES_INDEX_DICTIONARY_INDEX, {"name": name})[-1]
    dictionary_words_final = {}
    dictionary_normal_words = {}
    print("!!!", "Iteration through scan", datetime.datetime.now())
    for word in dictionary_scan:
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

    print("!!!", "Forming final words dict", datetime.datetime.now())
    for key in dictionary_words_final.keys():
        dictionary_words_final[key]['word_normal_frequency'] = \
        dictionary_normal_words[dictionary_words_final[key]['word_normal']]['word_normal_frequency']
        dictionary_words_final[key]['word_normal_first_capital_ratio'] = \
        dictionary_normal_words[dictionary_words_final[key]['word_normal']]['word_normal_first_capital_ratio']
        dictionary_words_final[key]['document_normal_frequency'] = \
        dictionary_normal_words[dictionary_words_final[key]['word_normal']]['document_normal_frequency']

        dictionary_words_final[key]['word_first_capital_ratio'] /= \
            dictionary_words_final[key]['word_frequency']
        dictionary_words_final[key]['word_normal_first_capital_ratio'] /= \
            dictionary_words_final[key]['word_normal_frequency']

        dictionary_words_final[key]['word_frequency_relative'] = \
            dictionary_words_final[key]['word_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['word_normal_frequency_relative'] = \
            dictionary_words_final[key]['word_normal_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['document_frequency_relative'] = \
            dictionary_words_final[key]['document_frequency'] / dictionary_index.number_of_documents
        dictionary_words_final[key]['document_normal_frequency_relative'] = \
            dictionary_words_final[key]['document_normal_frequency'] / dictionary_index.number_of_documents

    success = 0
    failed = 0
    print("!!!", "Writing to ES", datetime.datetime.now())
    len_dictionary = len(dictionary_words_final)
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("terms", corpus=corpuses).source([])[:0]
    number_of_documents = s.count()
    print("!!!", "Number of documents", number_of_documents)
    dictionary_words_final = filter(lambda x: x['document_frequency'] > number_of_documents * min_relative_document_frequency, dictionary_words_final.values())
    for ok, result in parallel_bulk(ES_CLIENT, dictionary_words_final,
                                    index=f"{ES_INDEX_DICTIONARY_WORD}_{name}",
                                    chunk_size=10000, raise_on_error=True, thread_count=6):
        if not ok:
            failed += 1
        else:
            success += 1
        if success % 10000 == 0:
            print(f"{success}/{len_dictionary} processed, {datetime.datetime.now()}")
        if failed > 3:
            raise Exception("Too many failed!!")
    ES_CLIENT.update(index=ES_INDEX_DICTIONARY_INDEX, id=dictionary_index.meta.id, body={"doc": {"is_ready": True}})
    es_index = Index(f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp", using=ES_CLIENT)
    es_index.delete(ignore=404)
    return success
