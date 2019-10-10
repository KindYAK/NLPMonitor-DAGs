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
    import datetime
    import numpy as np
    from elasticsearch.helpers import parallel_bulk

    from util.constants import BASE_DAG_DIR
    from util.service_es import update_generator

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_MODELLING
    from mainapp.documents import Document as ESDocument

    name = kwargs['name']
    regularization_params = kwargs['regularization_params']
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
    # Add scores
    model_artm.scores.add(artm.PerplexityScore(name='PerplexityScore'))
    model_artm.scores.add(artm.TopicKernelScore(name='TopicKernelScore', class_id='text', probability_mass_threshold=0.3))
    # Regularize
    model_artm.regularizers.add(artm.SmoothSparseThetaRegularizer(name='SparseTheta',
                                                                  tau=regularization_params['SmoothSparseThetaRegularizer']))
    model_artm.regularizers.add(artm.SmoothSparsePhiRegularizer(name='SparsePhi',
                                                                tau=regularization_params['SmoothSparsePhiRegularizer']))
    model_artm.regularizers.add(artm.DecorrelatorPhiRegularizer(name='DecorrelatorPhi',
                                                                tau=regularization_params['DecorrelatorPhiRegularizer']))
    model_artm.regularizers.add(artm.ImproveCoherencePhiRegularizer(name='ImproveCoherencePhi',
                                                                    tau=regularization_params['ImproveCoherencePhiRegularizer']))

    print("!!!", "Start model train", datetime.datetime.now())
    # Fit model
    model_artm.fit_offline(batch_vectorizer=batch_vectorizer, num_collection_passes=10)

    phi = model_artm.get_phi()
    print("!!!", "Get topics", datetime.datetime.now())
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

    # Add metrics
    purity = np.mean(model_artm.score_tracker['TopicKernelScore'].last_average_purity)
    contrast = np.mean(model_artm.score_tracker['TopicKernelScore'].last_average_contrast)
    coherence = np.mean(model_artm.score_tracker['TopicKernelScore'].average_coherence)
    perplexity = model_artm.score_tracker['PerplexityScore'].last_value

    print("!!!", "Write topics", datetime.datetime.now())
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=index.meta.id,
                             body={"doc": {
                                 "topics": topics,
                                 "purity": purity,
                                 "contrast": contrast,
                                 "coherence": coherence,
                                 "perplexity": perplexity,
                                 "tau_smooth_sparse_theta": regularization_params['SmoothSparseThetaRegularizer'],
                                 "tau_smooth_sparse_phi": regularization_params['SmoothSparsePhiRegularizer'],
                                 "tau_decorrelator_phi": regularization_params['DecorrelatorPhiRegularizer'],
                                 "tau_coherence_phi": regularization_params['ImproveCoherencePhiRegularizer'],
                             }
                         }
                     )

    print("!!!", "Get document-topics", datetime.datetime.now())
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

    print("!!!", "Write document-topics", datetime.datetime.now())
    for ok, result in parallel_bulk(ES_CLIENT, update_generator(ES_INDEX_DOCUMENT, documents), index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, thread_count=4, raise_on_error=True):
        pass
        # print(ok, result)
    print("!!!", "Done writing", datetime.datetime.now())
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=index.meta.id, body={"doc": {"is_ready": True}})
    return "Topic Modelling complete"
