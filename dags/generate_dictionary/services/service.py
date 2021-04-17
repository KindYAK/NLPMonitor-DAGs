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


def lemmatize_ru(word):
    from pymorphy2 import MorphAnalyzer
    morph = MorphAnalyzer()
    parse = morph.parse(word)
    return {
        "normal_form": parse[0].normal_form,
        "is_known": parse[0].is_known,
        "is_multiple_forms": len(parse) > 1,
        "pos_tag": parse[0].tag.POS,
    }


def lemmatize_eng(word):
    from lemminflect import getAllLemmas, getAllLemmasOOV
    result = ""
    is_known = True
    is_multiple_forms = False
    for w in word.split():
        try:
            result += list(getAllLemmas(w).values())[0][0] + " "
            if len(list(getAllLemmas(w).values())) > 1:
                is_multiple_forms = True
        except IndexError:
            is_known = False
            result += list(getAllLemmasOOV(w, upos="NOUN").values())[0][0] + " "
    return {
        "normal_form": result,
        "is_known": is_known,
        "is_multiple_forms": is_multiple_forms,
        "pos_tag": "UNKNW",
    }


def generate_dictionary_batch(**kwargs):
    import datetime
    import re

    from elasticsearch.helpers import streaming_bulk
    from stop_words import get_stop_words
    from nltk.corpus import stopwords

    from util.util import is_kazakh, is_latin
    from util.service_es import search

    from nlpmonitor.settings import ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    name = kwargs['name']
    process_num = kwargs['process_num']
    total_proc = kwargs['total_proc']
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
                       source=[field_to_parse, 'id'],
                       sort=['id'],
                       get_search_obj=True,
                       )
    documents = documents.filter("exists", field=field_to_parse)
    number_of_documents = documents.count()

    # stopwords = set(get_stop_words('ru') + get_stop_words('en') + stopwords.words('english'))
    dictionary_words = {}
    print("!!!", "Iterating through documents", datetime.datetime.now())
    for i, doc in enumerate(documents.params(raise_on_error=False).scan()):
        if i % 100_000 == 0:
            print(f"Processed {i} documents")
            print(f"Dictionary length is {len(dictionary_words)}")
        if int(doc.id) % total_proc != process_num:
            continue
        if len(doc[field_to_parse]) == 0:
            print("!!! WTF", doc.meta.id)
            continue
        if is_kazakh(doc[field_to_parse]):
            continue
        word_in_doc = set()
        cleaned_words = [x for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', doc[field_to_parse]).split()).split()]
        if is_latin(doc[field_to_parse]):
            lang = "eng"
        elif is_kazakh((doc[field_to_parse])):
            lang = "kaz"
        else:
            lang = "rus"
        for n_gram_len in range(1, max_n_gram_len + 1):
            for n_gram in (cleaned_words[i:i + n_gram_len] for i in range(len(cleaned_words) - n_gram_len + 1)):
                word = "_".join(n_gram)
                is_first_upper = word[0].isupper()
                word = word.lower()
                # TEMP - DISABLED lemmatization
                # if lang == "eng":
                #     parse = lemmatize_eng(word)
                # elif lang == "kaz":
                #     continue # raise NotImplemented()
                # elif lang == "rus":
                #     parse = lemmatize_ru(word)
                # else:
                #     raise NotImplemented()
                if word not in dictionary_words:
                    dictionary_words[word] = {
                        "dictionary": name,
                        "word": word,
                        # "word_normal": parse["normal_form"],
                        "word_normal": word,
                        # "is_in_pymorphy2_dict": parse["is_known"],
                        "is_in_pymorphy2_dict": True,
                        # "is_multiple_normals_in_pymorphy2": parse["is_multiple_forms"],
                        "is_multiple_normals_in_pymorphy2": False,
                        # "is_stop_word": word in stopwords or parse["normal_form"] in stopwords,
                        "is_stop_word": False,
                        "is_latin": any([c in "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM" for c in word]),
                        "is_kazakh": any([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in word]) or lang == "kaz",
                        "n_gram_len": n_gram_len,
                        # "pos_tag": parse["pos_tag"],
                        "pos_tag": "NA",
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
    dictionary_words = filter(lambda x: x['document_frequency'] > number_of_documents * min_relative_document_frequency, dictionary_words.values())
    success = 0
    failed = 0
    print("!!!", "Writing to ES", datetime.datetime.now())
    for ok, result in streaming_bulk(ES_CLIENT, dictionary_words, index=f"{ES_INDEX_DICTIONARY_WORD}_{name}_temp",
                                    chunk_size=1000, raise_on_error=True, max_retries=10):
        if not ok:
            failed += 1
        else:
            success += 1
        if success % 1000 == 0:
            print(f"{success}/{len_dictionary} processed, {datetime.datetime.now()}")
        if failed > 3:
            raise Exception("Too many failed!!")
    return number_of_documents


def aggregate_dicts(**kwargs):
    import datetime

    from util.service_es import search
    from elasticsearch.helpers import streaming_bulk
    from elasticsearch_dsl import Search, Index
    from nlpmonitor.settings import ES_INDEX_DICTIONARY_INDEX, ES_INDEX_DICTIONARY_WORD, ES_CLIENT, ES_INDEX_DOCUMENT

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

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
    print("!!! Min documents threshold", number_of_documents * min_relative_document_frequency)
    dictionary_words_final = filter(lambda x: x['document_frequency'] > number_of_documents * min_relative_document_frequency, dictionary_words_final.values())
    for ok, result in streaming_bulk(ES_CLIENT, dictionary_words_final,
                                    index=f"{ES_INDEX_DICTIONARY_WORD}_{name}",
                                    chunk_size=1000, raise_on_error=True, max_retries=10):
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
