def ngramize(**kwargs):
    import datetime

    from airflow.models import Variable
    from elasticsearch.helpers import streaming_bulk
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_CUSTOM_DICTIONARY_WORD, \
        ES_INDEX_DICTIONARY_WORD

    from util.service_es import search, update_generator

    process_num = kwargs['process_num']
    total_proc = kwargs['total_proc']
    corpus = kwargs['corpus']
    dict_name = kwargs['dict_name']
    source_field = kwargs['source_field']
    min_document_frequency_relative = kwargs['min_document_frequency_relative']
    max_n_gram_len = kwargs['max_n_gram_len']

    print("!!!", "Getting documents", datetime.datetime.now())
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query={}, source=(source_field, 'id'), sort=('id',), get_search_obj=True)
    documents = documents.exclude('exists', field=f'text_ngramized_{dict_name}')
    documents = documents.filter('exists', field=source_field)
    documents = documents.filter('terms', corpus=corpus)

    print("!!!", "Getting dictionary", datetime.datetime.now())
    s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DICTIONARY_WORD}_{dict_name}")
    s = s.filter("range", document_frequency_relative={"gt": min_document_frequency_relative})
    s = s.filter("range", n_gram_len={"gte": 2})
    s = s.source(("word",))
    dict_words = set(w.word for w in s.scan())
    print('!!! len dict', len(dict_words))

    print("!!!", "Processing documents", datetime.datetime.now())
    success = 0
    for doc in documents.params(raise_on_error=False).scan():
        if int(doc.id) % total_proc != process_num:
            continue
        success += 1
        if success > 10_000:
            break
        if success % 1_000 == 0:
            print(f"{success}/{10_000}")
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
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        if not ok:
            failed += 1
        else:
            success += 1
        if failed > 10:
            raise Exception("Too many failed ES!!!")
        documents_processed += 1
    return f"{documents_processed} processed"
