from util.util import is_kazakh
from .calc_topics_info import calc_topics_info


def bigartm_calc(**kwargs):
    dataset_prepare_result = dataset_prepare(**kwargs)
    if dataset_prepare_result == 1:
        return "TopicsGroup is empty"
    print("!#!#!#!#", "Dataset Prepare returned: ", dataset_prepare_result)
    print("!#!#!#!#", "Topic modelling Calc returned: ", topic_modelling(**kwargs))
    print("!#!#!#!#", "Calc topics info returned: ", calc_topics_info(kwargs['corpus'],
                                                                      kwargs['name'],
                                                                      kwargs['topic_weight_threshold']
                                                                      )
          )


class TMNotFoundException(Exception):
    pass


def init_tm_index(**kwargs):
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
    from mainapp.documents import TopicModellingIndex

    corpus = kwargs['corpus']
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']

    # Check if already exists
    try:
        return get_tm_index(**kwargs)
    except TMNotFoundException:
        pass

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", corpus=corpus)
    if source:
        s = s.filter("term", **{"source": source})
    if datetime_from:
        s = s.filter('range', datetime={'gte': datetime_from})
    if datetime_to:
        s = s.filter('range', datetime={'lt': datetime_to})
    number_of_documents = s.count()

    kwargs["number_of_documents"] = number_of_documents
    kwargs["is_ready"] = False
    index = TopicModellingIndex(**kwargs)
    index.save()
    return index


def get_tm_index(**kwargs):
    from util.service_es import search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING

    name = kwargs['name']
    corpus = kwargs['corpus']

    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_TOPIC_MODELLING):
        query = {
            "name": name,
            "corpus": corpus,
        }
        s = search(ES_CLIENT, ES_INDEX_TOPIC_MODELLING, query, source=[])
        if s:
            return s[-1]
        query = {
            "name.keyword": name,
            "corpus": corpus,
        }
        s = search(ES_CLIENT, ES_INDEX_TOPIC_MODELLING, query, source=[])
        if s:
            return s[-1]
    raise TMNotFoundException("Topic Modelling index not found!")


def dataset_prepare(**kwargs):
    import os
    import shutil
    import artm
    import datetime
    from elasticsearch_dsl import Search

    from dags.bigartm.services.cleaners import return_cleaned_array, txt_writer
    from util.constants import BASE_DAG_DIR

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.models_user import TopicGroup

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    index = init_tm_index(**kwargs)

    lc = artm.messages.ConfigureLoggingArgs()
    lib = artm.wrapper.LibArtm(logging_config=lc)
    lc.minloglevel = 3  # 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL
    lib.ArtmConfigureLogging(lc)

    perform_actualize = 'perform_actualize' in kwargs
    name = kwargs['name']
    name_translit = kwargs['name_translit']
    corpus = kwargs['corpus']
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    group_id = kwargs['group_id']
    topic_weight_threshold = kwargs['topic_weight_threshold']
    # Extract
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", corpus=corpus).filter('exists', field="text_lemmatized")
    if source:
        s = s.filter("term", **{"source": source})
    if datetime_from:
        s = s.filter('range', datetime={'gte': datetime_from})
    if datetime_to and not perform_actualize:
        s = s.filter('range', datetime={'lt': datetime_to})
    group_document_es_ids = None
    if group_id:
        group = TopicGroup.objects.get(id=group_id)
        topic_ids = [t.topic_id for t in group.topics.all()]
        if not topic_ids:
            return 1
        topic_modelling_name = group.topic_modelling_name
        st = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling_name}")\
            .filter("terms", **{"topic_id": topic_ids})\
            .filter("range", topic_weight={"gte": topic_weight_threshold}) \
            .filter("range", datetime={"gte": datetime.date(2000, 1, 1)}) \
            .source(('document_es_id'))[:1000000]
        r = st.scan()
        group_document_es_ids = set([doc.document_es_id for doc in r])

    # Exclude document already in TM if actualizing
    ids_to_skip = None
    if perform_actualize:
        print("!!!", "Performing actualizing, skipping document already in TM")
        std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{name}").source([])[:0]
        std.aggs.bucket(name="ids", agg_type="terms", field="document_es_id", size=5000000)
        r = std.execute()
        ids_to_skip = set([bucket.key for bucket in r.aggregations.ids.buckets])

    s = s.source(["id", "text_lemmatized", "title", "source", "datetime"]).sort(('id',))[:5000000]
    ids = []
    texts = []
    titles = []
    sources = []
    dates = []
    ids_in_list = set()
    for document in s.scan():
        if document.meta.id in ids_in_list:
            continue
        if ids_to_skip is not None and document.meta.id in ids_to_skip:
            continue
        if group_document_es_ids is not None and document.meta.id not in group_document_es_ids:
            continue
        if is_kazakh(document.text_lemmatized + document.title if document.title else ""):
            continue
        ids.append(document.meta.id)
        ids_in_list.add(document.meta.id)
        texts.append(document.text_lemmatized)
        titles.append(document.title)
        sources.append(document.source)
        dates.append(document.datetime if hasattr(document, "datetime") and document.datetime else "")
    titles = return_cleaned_array(titles)

    formated_data = []
    for id, text, title, source, date in zip(ids, texts, titles, sources, dates):
        formated_data.append(f'{id}*{source.replace(" ", "_")}*{date}' + ' ' +
                                   '|text' + ' ' + text + ' ' +
                                   '|title' + ' ' + title + ' ')

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}")
    shutil.rmtree(data_folder, ignore_errors=True)
    os.mkdir(data_folder)
    if perform_actualize and len(formated_data) == 0:
        return f"No documents to actualize"
    print("!!!", f"Writing {len(formated_data)} documents")
    txt_writer(data=formated_data, filename=os.path.join(data_folder, f"bigartm_formated_data.txt"))
    artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data.txt"),
                                            data_format="vowpal_wabbit",
                                            target_folder=os.path.join(data_folder, "batches"))
    # TODO ngrams dictionary
    return f"index.number_of_document={index.number_of_documents}, len(ids)={len(ids)}"


def topic_modelling(**kwargs):
    import artm
    import glob
    import os
    import datetime
    import numpy as np
    import shutil
    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Index, Search
    from numba import jit

    from util.constants import BASE_DAG_DIR

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.documents import TopicDocument

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    perform_actualize = 'perform_actualize' in kwargs
    name = kwargs['name']
    name_translit = kwargs['name_translit']
    regularization_params = kwargs['regularization_params']
    is_actualizable = 'is_actualizable' in kwargs and kwargs['is_actualizable']
    index = get_tm_index(**kwargs)

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}")
    batches_folder = os.path.join(data_folder, "batches")
    if perform_actualize and not os.path.exists(batches_folder):
        return f"No documents to actualize"
    batch_vectorizer = artm.BatchVectorizer(data_path=batches_folder,
                                            data_format='batches')
    dictionary = artm.Dictionary()
    dictionary.gather(batch_vectorizer.data_path)

    model_folder = os.path.join(BASE_DAG_DIR, "bigartm_models")
    model_artm = artm.ARTM(num_topics=index.number_of_topics,
                           class_ids={"text": 1}, theta_columns_naming="title",
                           reuse_theta=True, cache_theta=True, num_processors=4)
    if not perform_actualize:
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
        if not os.path.exists(model_folder):
            os.mkdir(model_folder)
        model_artm.save(os.path.join(model_folder, f"model_{name if not name_translit else name_translit}.model"))

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

    else:
        print("!!!", "Loading existing model")
        # Monkey patching stupid BigARTM bug
        def load(self, filename, model_name="p_wt"):
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

        model_artm.load = load
        model_artm.load(model_artm, os.path.join(model_folder, f"model_{name if not name_translit else name_translit}.model"))

    print("!!!", "Get document-topics", datetime.datetime.now())
    if not perform_actualize:
        theta = model_artm.get_theta()
    else:
        theta = model_artm.transform(batch_vectorizer)
    theta_values = theta.values.transpose().astype(float)
    theta_topics = theta.index.array.to_numpy().astype(str)
    theta_documents = theta.columns.array.to_numpy().astype(str)
    # Assign topics to docs in ES
    @jit(nopython=True)
    def topic_document_generator(theta_values, theta_documents):
        for i, document in enumerate(theta_documents):
            yield document, theta_values[i]

    def topic_document_generator_converter(d, row):
        id, source, date = d.split("*")
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
                    es_topic_document.datetime = datetime.datetime.strptime(date[:-3] + date[-2:], "%Y-%m-%dT%H:%M:%S%z")
                except:
                    es_topic_document.datetime = datetime.datetime.strptime(date[:-3] + date[-2:], "%Y-%m-%dT%H:%M:%S.%f%z")
            es_topic_document.document_source = source.replace("_", " ")
            document_topics.append(es_topic_document)
        document_topics = sorted(document_topics, key=lambda x: x.topic_weight, reverse=True)[:max(index.number_of_topics // 3, 10)]
        for es_topic_document in document_topics:
            yield es_topic_document

    print("!!!", "Write document-topics", datetime.datetime.now())
    if not perform_actualize:
        es_index = Index(f"{ES_INDEX_TOPIC_DOCUMENT}_{name}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_TOPIC_DOCUMENT}_{name}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_TOPIC_DOCUMENT}_{name}", body={
                "settings": TopicDocument.Index.settings,
                "mappings": TopicDocument.Index.mappings
            }
        )

    success, failed = 0, 0
    batch_size = 100000
    time_start = datetime.datetime.now()
    row_generator = (topic_document_generator_converter(id, row) for id, row in topic_document_generator(theta_values, theta_documents))
    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for row in row_generator for doc in row),
                                    index=f"{ES_INDEX_TOPIC_DOCUMENT}_{name}", chunk_size=batch_size, thread_count=10, raise_on_error=True):
        if ok:
            success += 1
        else:
            print("!!!", "ES index fail, error", result)
            failed += 1
        if failed > 3:
            raise Exception("Too many failed to ES!!")
        if (success + failed) % batch_size == 0:
            minutes = round((datetime.datetime.now() - time_start).seconds / 60, 2)
            print(f'{success + failed} / {index.number_of_documents * index.number_of_topics} processed, '
                  f'took {minutes} min, TETA~{round(minutes * index.number_of_documents * index.number_of_topics / batch_size / 60, 2)} hours')
            time_start = datetime.datetime.now()
    print("!!!", "Done writing", datetime.datetime.now())
    s = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{name}").source([])[:0]
    s.aggs.bucket(name="ids", agg_type="terms", field="document_es_id", size=5000000)
    r = s.execute()
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=index.meta.id,
                         body={
                             "doc": {
                                 "is_ready": True,
                                 "number_of_documents": len(r.aggregations.ids.buckets),
                                 "is_actualizable": is_actualizable,
                             }
                         }
                     )
    # Remove logs
    fileList = glob.glob(f'{BASE_DAG_DIR}/bigartm.*')
    for filePath in fileList:
        os.remove(filePath)
    # Remove batches and stuff
    shutil.rmtree(data_folder, ignore_errors=True)
    return theta_documents.shape[0]
