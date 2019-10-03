def init_embedding_index(**kwargs):
    from util.service_es import search, get_count
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_MODELLING
    from mainapp.documents import TopicModellingIndex

    name = kwargs['name']
    corpus = kwargs['corpus']
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", **{"corpus": corpus})
    if source:
        s = s.filter("term", source=source)
    if datetime_from:
        s = s.filter('range', datetime={'gte': datetime_from})
    if datetime_to:
        s = s.filter('range', datetime={'lt': datetime_to})
    number_of_documents = s.count()

    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_TOPIC_MODELLING):
        query = {
            "name": name,
            "corpus": corpus,
        }
        if source:
            query["source.keyword"] = source
        if datetime_from:
            query['datetime_from'] = datetime_from
        if datetime_to:
            query['datetime_to'] = datetime_to
        s = search(ES_CLIENT, ES_INDEX_TOPIC_MODELLING, query, source=["number_of_topics", "number_of_documents"])
        if s:
            return s[-1]

    kwargs["number_of_documents"] = number_of_documents
    index = TopicModellingIndex(**kwargs)
    index.save()
    return index


def dataset_prepare(**kwargs):
    import os
    import artm
    from elasticsearch_dsl import Search

    from dags.bigartm.bigartm.cleaners import return_cleaned_array, txt_writer
    from util.constants import BASE_DAG_DIR
    from util.service_es import search, update_generator, get_count

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_MODELLING

    index = init_embedding_index(**kwargs)

    lc = artm.messages.ConfigureLoggingArgs()
    lib = artm.wrapper.LibArtm(logging_config=lc)
    lc.minloglevel = 3  # 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL
    lib.ArtmConfigureLogging(lc)

    name = kwargs['name']
    corpus = kwargs['corpus']
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    # Extract
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", **{"corpus": corpus}).filter('exists', field="text_lemmatized")
    if source:
        s = s.filter("term", source=source)
    if datetime_from:
        s = s.filter('range', datetime={'gte': datetime_from})
    if datetime_to:
        s = s.filter('range', datetime={'lt': datetime_to})
    s = s.source(["id", "text_lemmatized", "title", "source", "datetime"]).sort(('id',))[:index.number_of_documents]
    ids = []
    texts = []
    titles = []
    sources = []
    dates = []
    for document in s.scan():
        ids.append(document.meta.id)
        texts.append(document.text_lemmatized)
        titles.append(document.title)
        sources.append(document.source)
        dates.append(document.datetime if hasattr(document, "datetime") else "")
    titles = return_cleaned_array(titles)
    sources = return_cleaned_array(sources)
    dates = return_cleaned_array(dates)

    formated_data = []
    for id, text, title, source, date in zip(ids, texts, titles, sources, dates):
        formated_data.append(f'{id}' + ' ' +
                                   '|text' + ' ' + text + ' ' +
                                   '|title' + ' ' + title + ' ' +
                                   '|source' + ' ' + source + ' ' +
                                   '|date' + ' ' + date)

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name}")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    txt_writer(data=formated_data, filename=os.path.join(data_folder, f"bigartm_formated_data.txt"))
    artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data.txt"),
                                            data_format="vowpal_wabbit",
                                            target_folder=os.path.join(data_folder, "batches"))
    # TODO ngrams dictionary
    return "Preprocessing complete"


def topic_modelling(**kwargs):
    import artm
    import os
    from elasticsearch.helpers import streaming_bulk

    from util.constants import BASE_DAG_DIR
    from util.service_es import update_generator

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_MODELLING
    from mainapp.documents import Document as ESDocument

    name = kwargs['name']
    index = init_embedding_index(**kwargs)

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name}")
    batch_vectorizer = artm.BatchVectorizer(data_path=os.path.join(data_folder, "batches"),
                                            data_format='batches')
    dictionary = artm.Dictionary()
    dictionary.gather(batch_vectorizer.data_path)

    model_artm = artm.ARTM(num_topics=index.number_of_topics,
                           class_ids={"text": 1}, theta_columns_naming="title",
                           reuse_theta=True, cache_theta=True, num_processors=4)
    model_artm.initialize(dictionary)
    # fit model
    model_artm.fit_offline(batch_vectorizer=batch_vectorizer, num_collection_passes=10)

    phi = model_artm.get_phi()
    # Create topics in ES
    topics = []
    for topic in phi:
        phi_filtered = phi[phi[topic] > 0.0001]
        topic_words = [
            {
                "word": ind[1],
                "weight": float(phi[topic][ind])
            }
            for ind in phi_filtered[topic].index
        ]
        topics.append({
            "id": topic,
            "topic_words": sorted(topic_words, key=lambda x: x['weight'], reverse=True)[:100]
        })

    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=index.meta.id, body={"doc": {"topics": topics}})
    theta = model_artm.get_theta()
    # Assign topics to docs in ES
    documents = []
    for document in theta:
        es_document = ESDocument()
        es_document.meta['id'] = document
        theta_filtered = theta[theta[document] > 0.0001]
        document_topics = [
            {
                "topic": ind,
                "weight": float(theta[document][ind])
            } for ind in theta_filtered[document].index
        ]

        es_document[f'topics_{name}'] = sorted(document_topics, key=lambda x: x['weight'], reverse=True)[:100]
        documents.append(es_document)

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents), index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        pass
        # print(ok, result)
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=index.meta.id, body={"doc": {"is_ready": True}})
    return "Topic Modelling complete"
