from util.util import is_kazakh

def bigartm_calc(**kwargs):
    dataset_prepare_result = dataset_prepare(**kwargs)
    if dataset_prepare_result == 1:
        return "TopicsGroup is empty"
    print("!#!#!#!#", "Dataset Prepare returned: ", dataset_prepare_result)
    print("!#!#!#!#", "Topic modelling Calc returned: ", topic_modelling(**kwargs))


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

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp_to_delete")
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

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp_to_delete")
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
        theta = model_artm.get_theta()
        import pickle
        with open(os.path.join(model_folder, "phi.pickle"), "wb") as f:
            pickle.dump(phi, f)
        with open(os.path.join(model_folder, "theta.pickle"), "wb") as f:
            pickle.dump(theta, f)
    return "Done"
