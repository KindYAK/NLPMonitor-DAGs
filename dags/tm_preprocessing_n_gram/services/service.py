def init_last_datetime(**kwargs):
    from airflow.models import Variable
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    dict_name = kwargs['dict_name']
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    s = s.exclude('exists', field=f'text_ngramized_{dict_name}')
    Variable.set(f"ngramize_number_of_documents_{dict_name}", s.count())


def ngramize(**kwargs):
    import datetime

    from airflow.models import Variable
    from elasticsearch.helpers import streaming_bulk
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_CUSTOM_DICTIONARY_WORD, \
        ES_INDEX_DICTIONARY_WORD

    from util.service_es import search, update_generator

    start = kwargs['start']
    end = kwargs['end']
    dict_name = kwargs['dict_name']
    source_field = kwargs['source_field']
    min_document_frequency_relative = kwargs['min_document_frequency_relative']
    max_n_gram_len = kwargs['max_n_gram_len']

    number_of_documents = int(Variable.get(f"ngramize_number_of_documents_{dict_name}", default_var=None))
    if number_of_documents is None:
        raise Exception("No variable!")

    print("!!!", "Getting documents", datetime.datetime.now())
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=(source_field,), sort=('id',), get_search_obj=True)
    documents = documents.exclude('exists', field=f'text_ngramized_{dict_name}')
    documents = documents.filter('exists', field=source_field)
    start = int(start / 100 * number_of_documents)
    end = int(end / 100 * number_of_documents) + 1
    if start - end > 30_000:
        end = start + 30_000
    documents = documents[start:end].execute()
    print('!!! len docs', len(documents))

    print("!!!", "Getting dictionary", datetime.datetime.now())
    s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DICTIONARY_WORD}_{dict_name}")
    s = s.filter("range", document_frequency_relative={"gt": min_document_frequency_relative})
    s = s.filter("range", n_gram_len={"gte": 2})
    s = s.source(("word",))
    dict_words = set(w.word for w in s.scan())
    print('!!! len dict', len(dict_words))

    print("!!!", "Processing documents", datetime.datetime.now())
    for doc in documents:
        text_ngramized = doc[source_field]
        text_ngramized_split = text_ngramized.split()
        n_grams_to_append = []
        for n_gram_len in range(2, max_n_gram_len + 1):
            n_grams = [text_ngramized_split[i:i + n_gram_len] for i in range(len(text_ngramized_split) - n_gram_len + 1)]
            for n_gram in n_grams:
                word = "_".join(n_gram)
                if word in dict_words:
                    n_grams_to_append.append(word)
        doc[f'text_ngramized_{dict_name}'] = text_ngramized + " ".join(n_grams_to_append)

    print("!!!", "Writing to ES", datetime.datetime.now())
    documents_processed = 0
    success = 0
    failed = 0
    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents),
                                     index=ES_INDEX_DOCUMENT,
                                     chunk_size=5000, raise_on_error=True, max_retries=10):
        if not ok:
            failed += 1
        else:
            success += 1
        if success % 5000 == 0:
            print(f"{success}/{len(documents)} proessed, {datetime.datetime.now()}")
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        documents_processed += 1
    return f"{documents_processed}/{len(documents)} processed"
