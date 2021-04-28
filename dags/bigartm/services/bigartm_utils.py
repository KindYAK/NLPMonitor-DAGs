def send_tds_to_es_wrapper(model_artm, perform_actualize, tm_index, batch_vectorizer,
                           topic_doc, name, is_dynamic, is_actualizable, index_tm):
    import datetime
    from elasticsearch_dsl import Index

    from util.util import shards_mapping

    from nlpmonitor.settings import ES_CLIENT
    from mainapp.documents import TopicDocument, TopicDocumentUniqueIDs

    print("!!!", "Get document-topics", datetime.datetime.now())

    print("!!!", "Write document-topics", datetime.datetime.now())
    if not perform_actualize:
        es_index = Index(f"{topic_doc}_{name}", using=ES_CLIENT)
        es_index.delete(ignore=404)

    if not perform_actualize:
        theta = model_artm.get_theta(topic_names=["topic_0"])
    else:
        theta = model_artm.transform(batch_vectorizer)
    theta_documents = theta.columns.array.to_numpy().astype(str)
    number_of_documents = len(theta_documents)
    print("!!!!!", f"Documents {number_of_documents}, topics {tm_index.number_of_topics}")
    if not ES_CLIENT.indices.exists(f"{topic_doc}_{name}"):
        settings = TopicDocument.Index.settings if not is_dynamic else TopicDocument.Index.settings_dynamic
        settings['number_of_shards'] = shards_mapping(tm_index.number_of_topics * number_of_documents)
        ES_CLIENT.indices.create(index=f"{topic_doc}_{name}", body={
            "settings": settings,
            "mappings": TopicDocument.Index.mappings
        }
    )

    documents_per_iteration = 1_500_000 // (tm_index.number_of_topics // 250 + 1)
    n_iterations = (number_of_documents // documents_per_iteration + 1) if not perform_actualize else 1
    for i in range(n_iterations):
        print("!!!", f"Iteration {i} / {n_iterations}", datetime.datetime.now())
        start = (100 / n_iterations) * i
        end = (100 / n_iterations) * (i + 1)
        if not perform_actualize:
            theta = model_artm.get_theta([f"topic_{j}" for j in range(int(start / 100 * tm_index.number_of_topics),
                                                                      int(end / 100 * tm_index.number_of_topics))])
        send_tds_to_es(theta, topic_doc, name, tm_index, n_iterations=n_iterations)
    print("!!!", "Done writing", datetime.datetime.now())

    if perform_actualize:
        number_of_documents += tm_index.number_of_documents
    ES_CLIENT.update(index=index_tm, id=tm_index.meta.id,
                     body={
                         "doc": {
                             "is_ready": True,
                             "number_of_documents": number_of_documents,
                             "is_actualizable": is_actualizable,
                         }
                     }
                     )
    return theta_documents


def send_tds_to_es(theta, topic_doc, name, tm_index, n_iterations=1):
    import datetime
    from elasticsearch.helpers import parallel_bulk
    from numba import jit

    from nlpmonitor.settings import ES_CLIENT
    from mainapp.documents import TopicDocument, TopicDocumentUniqueIDs

    theta_values = theta.values.transpose().astype(float)
    theta_topics = theta.index.array.to_numpy().astype(str)
    theta_documents = theta.columns.array.to_numpy().astype(str)

    # Assign topics to docs in ES
    @jit(nopython=True)
    def topic_document_generator(theta_values, theta_documents):
        for i, document in enumerate(theta_documents):
            yield document, theta_values[i]

    def topic_document_generator_converter(d, row):
        id, source, date, corpus, num_views, num_comments = d.split("*")
        document_topics = []
        for j, ind in enumerate(theta_topics):
            es_topic_document = TopicDocument()
            if float(row[j]) < 0.0001:
                continue
            es_topic_document.topic_id = ind
            es_topic_document.topic_weight = float(row[j])
            es_topic_document.document_es_id = id
            if date:
                try:
                    es_topic_document.datetime = datetime.datetime.strptime(date[:-3] + date[-2:],
                                                                            "%Y-%m-%dT%H:%M:%S%z")
                except:
                    es_topic_document.datetime = datetime.datetime.strptime(date[:-3] + date[-2:],
                                                                            "%Y-%m-%dT%H:%M:%S.%f%z")
            es_topic_document.document_source = source.replace("_", " ")
            es_topic_document.document_corpus = corpus
            es_topic_document.document_num_views = num_views
            es_topic_document.document_num_comments = num_comments
            document_topics.append(es_topic_document)
        document_topics = sorted(document_topics, key=lambda x: x.topic_weight, reverse=True)[:max(tm_index.number_of_topics // 3 // n_iterations, 10 // n_iterations)]
        for es_topic_document in document_topics:
            yield es_topic_document

    success, failed = 0, 0
    batch_size = 25_000
    row_generator = (topic_document_generator_converter(id, row) for id, row in
                     topic_document_generator(theta_values, theta_documents))
    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for row in row_generator for doc in row),
                                    index=f"{topic_doc}_{name}", chunk_size=batch_size, thread_count=5,
                                    raise_on_error=True):
        if ok:
            success += 1
        else:
            print("!!!", "ES index fail, error", result)
            failed += 1
        if failed > 3:
            raise Exception("Too many failed to ES!!")
        if (success + failed) % batch_size == 0:
            print(f'{success + failed} / {len(theta_documents) * len(theta_topics) // 3} processed', datetime.datetime.now())


def send_unique_ids_to_es(theta_documents, tm_index, is_dynamic, perform_actualize,
                          to_date, datetime_to, uniq_topic_doc, name):
    import datetime
    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Index

    from util.util import shards_mapping

    from nlpmonitor.settings import ES_CLIENT
    from mainapp.documents import TopicDocument, TopicDocumentUniqueIDs

    # Create or update unique IDS index
    print("!!!", "Writing unique IDs", datetime.datetime.now())
    if not is_dynamic or (is_dynamic and to_date == datetime_to):
        if not perform_actualize:
            es_index = Index(f"{uniq_topic_doc}_{name}", using=ES_CLIENT)
            es_index.delete(ignore=404)
        if not ES_CLIENT.indices.exists(f"{uniq_topic_doc}_{name}"):
            settings = TopicDocumentUniqueIDs.Index.settings
            settings['number_of_shards'] = shards_mapping(tm_index.number_of_topics * len(theta_documents))
            ES_CLIENT.indices.create(index=f"{uniq_topic_doc}_{name}", body={
                "settings": settings,
                "mappings": TopicDocumentUniqueIDs.Index.mappings
            }
         )

        def unique_ids_generator(theta_documents):
            for d in theta_documents:
                doc_id, _, _, _, _, _ = d.split("*")
                doc = TopicDocumentUniqueIDs()
                doc.document_es_id = doc_id
                yield doc

        batch_size = 50000
        success, failed = 0, 0
        for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in unique_ids_generator(theta_documents)),
                                        index=f"{uniq_topic_doc}_{name}", chunk_size=batch_size,
                                        thread_count=5, raise_on_error=True):
            if ok:
                success += 1
            else:
                print("!!!", "ES index fail, error", result)
                failed += 1
            if failed > 3:
                raise Exception("Too many failed to ES!!")
            if (success + failed) % batch_size == 0:
                print(f'{success + failed} / {tm_index.number_of_documents} processed')
        print("!!!", "Done writing", datetime.datetime.now())


def model_train(batches_folder, models_folder_name, perform_actualize, tm_index, regularization_params, name, name_translit, index_tm):
    import artm
    import os
    import datetime
    import numpy as np

    from util.constants import BASE_DAG_DIR

    from nlpmonitor.settings import ES_CLIENT

    print("Initializing vectorizer, model")
    batch_vectorizer = artm.BatchVectorizer(data_path=batches_folder,
                                            data_format='batches')
    model_folder = os.path.join(BASE_DAG_DIR, models_folder_name)
    model_artm = artm.ARTM(num_topics=tm_index.number_of_topics,
                           class_ids={"text": 1}, theta_columns_naming="title",
                           reuse_theta=True, cache_theta=True, num_processors=4)
    if not perform_actualize:
        dictionary = artm.Dictionary()
        if "scopus" in name and os.path.exists(os.path.join("/big_data/", "scopus250k.dict")):
            print("Loading dictionary")
            dictionary.load(os.path.join("/big_data/", "scopus250k.dict"))
        else:
            print("Gathering dictionary")
            dictionary.gather(batch_vectorizer.data_path, symmetric_cooc_values=True)
            print("Filtering dictionary")
            dictionary.filter(max_dictionary_size=250_000)
            if "scopus" in name and not os.path.exists(os.path.join("/big_data/", "scopus250k.dict")):
                print("Saving dictionary")
                dictionary.save(os.path.join("/big_data/", "scopus250k.dict"))

        print("Model - initial settings")
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
        for i in range(10):
            print(f"Pass #{i}")
            model_artm.fit_online(batch_vectorizer=batch_vectorizer)
        if not os.path.exists(model_folder):
            os.mkdir(model_folder)
        model_artm.save(os.path.join(model_folder,
                                     f"model_{name if not name_translit else name_translit}.model"))

        print("!!!", "Get topics", datetime.datetime.now())
        # Create topics in ES
        topics = []
        phi = model_artm.get_phi()
        for topic in phi:
            phi_filtered = phi[phi[topic] > 0.0001]
            topic_words = [
                {
                    "word": ind[1],
                    "weight": float(phi[topic][ind])
                }
                for ind in phi_filtered[topic].index
            ]
            topic_words = sorted(topic_words, key=lambda x: x['weight'], reverse=True)[:100]
            topics.append({
                "id": topic,
                "topic_words": topic_words,
                "name": ", ".join([w['word'] for w in topic_words[:5]])
            })

        # Add metrics
        purity = np.mean(model_artm.score_tracker['TopicKernelScore'].last_average_purity)
        contrast = np.mean(model_artm.score_tracker['TopicKernelScore'].last_average_contrast)
        coherence = np.mean(model_artm.score_tracker['TopicKernelScore'].average_coherence)
        perplexity = model_artm.score_tracker['PerplexityScore'].last_value
        print("!!!", "Write topics", datetime.datetime.now())
        update_body = {
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
        ES_CLIENT.update(index=index_tm,
                         id=tm_index.meta.id,
                         body={"doc": update_body}
                         )
    else:
        print("!!!", "Loading existing model")
        # Monkey patching stupid BigARTM bug
        model_artm.load = load_monkey_patch
        model_artm.load(model_artm, os.path.join(model_folder, f"model_{name if not name_translit else name_translit}.model"))
    return model_artm, batch_vectorizer


def load_monkey_patch(self, filename, model_name="p_wt"):
    _model_name = None
    if model_name == 'p_wt':
        _model_name = self.model_pwt
    elif model_name == 'n_wt':
        _model_name = self.model_nwt

    self.master.import_model(_model_name, filename)
    self._initialized = True

    config = self._lib.ArtmRequestMasterModelConfig(self.master.master_id)
    self._topic_names = list(config.topic_name)

    class_ids = {}
    for class_id in config.class_id:
        class_ids[class_id] = 1.0
    self._class_ids = class_ids

    if hasattr(config, 'transaction_typename'):
        transaction_typenames = {}
        for transaction_typename in config.transaction_typename:
            transaction_typenames[transaction_typename] = 1.0
        self._transaction_typenames = transaction_typenames

    # Remove all info about previous iterations
    self._score_tracker = {}
    self._synchronizations_processed = 0
    self._num_online_processed_batches = 0
    self._phi_cached = None
