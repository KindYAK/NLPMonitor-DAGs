def bigartm_calc(**kwargs):
    from dags.bigartm.services.calc_topics_info import calc_topics_info

    from nlpmonitor.settings import ES_INDEX_DYNAMIC_TOPIC_MODELLING, ES_INDEX_TOPIC_DOCUMENT, \
        ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS, ES_INDEX_DYNAMIC_TOPIC_DOCUMENT_UNIQUE_IDS, ES_INDEX_DYNAMIC_TOPIC_DOCUMENT, \
        ES_INDEX_TOPIC_MODELLING
    kwargs = kwargs.copy()
    is_dynamic = 'is_dynamic' in kwargs and kwargs['is_dynamic']
    if is_dynamic:
        kwargs['index_tm'] = ES_INDEX_DYNAMIC_TOPIC_MODELLING
        kwargs['topic_doc'] = ES_INDEX_DYNAMIC_TOPIC_DOCUMENT
        kwargs['uniq_topic_doc'] = ES_INDEX_DYNAMIC_TOPIC_DOCUMENT_UNIQUE_IDS
        kwargs['temp_folder'] = 'dynamic_bigartm_temp'
        kwargs['models_folder'] = 'dynamic_bigartm_models'
        kwargs['name'] = kwargs['name'] + "_" + str(kwargs['datetime_from'].date()) + \
                                    "_" + str(kwargs['datetime_to'].date())
        if kwargs['name_translit']:
            kwargs["name_translit"] = kwargs["name_translit"] + "_" + \
                                      str(kwargs['datetime_from'].date()) + \
                                      "_" + str(kwargs['datetime_to'].date())
    else:
        kwargs['index_tm'] = ES_INDEX_TOPIC_MODELLING
        kwargs['topic_doc'] = ES_INDEX_TOPIC_DOCUMENT
        kwargs['uniq_topic_doc'] = ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS
        kwargs['temp_folder'] = 'bigartm_temp'
        kwargs['models_folder'] = 'bigartm_models'
    dataset_prepare_result = dataset_prepare(**kwargs)
    if dataset_prepare_result == 1:
        return dataset_prepare_result
    print("!#!#!#!#", "Dataset Prepare returned: ", dataset_prepare_result)
    print("!#!#!#!#", "Topic modelling Calc returned: ", topic_modelling(**kwargs))
    if 'perform_actualize' not in kwargs and not is_dynamic:
        print("!#!#!#!#", "Calc topics info returned: ", calc_topics_info(
                                                                          kwargs['corpus'],
                                                                          kwargs['name'],
                                                                          kwargs['topic_weight_threshold'],
                                                                          )
              )


class TMNotFoundException(Exception):
    pass


def init_tm_index(**kwargs):
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
    from mainapp.documents import TopicModellingIndex, DynamicTopicModellingIndex

    kwargs = kwargs.copy()
    corpus = kwargs['corpus']
    kwargs['is_multi_corpus'] = True
    if type(corpus) != list:
        corpus = [corpus]
        kwargs['is_multi_corpus'] = False
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    is_dynamic = 'is_dynamic' in kwargs and kwargs['is_dynamic']

    # Check if already exists
    try:
        return get_tm_index(**kwargs)
    except TMNotFoundException:
        pass

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("terms", corpus=corpus)
    if source:
        s = s.filter("term", **{"source": source})
    if datetime_from:
        s = s.filter('range', datetime={'gte': datetime_from})
    if datetime_to:
        s = s.filter('range', datetime={'lt': datetime_to})
    number_of_documents = s.count()

    kwargs["number_of_documents"] = number_of_documents
    kwargs["is_ready"] = False
    kwargs['corpus'] = "_".join(corpus)
    if is_dynamic:
        index = DynamicTopicModellingIndex(**kwargs)
    else:
        index = TopicModellingIndex(**kwargs)
    index.save()
    return index


def get_tm_index(**kwargs):
    from util.service_es import search
    from nlpmonitor.settings import ES_CLIENT
    name = kwargs['name']
    corpus = kwargs['corpus']
    if type(corpus) == list:
        corpus = "_".join(corpus)
    index_tm = kwargs['index_tm']

    # Check if already exists
    if ES_CLIENT.indices.exists(index_tm):
        query = {
            "name": name,
            "corpus": corpus,
        }

        s = search(ES_CLIENT, index_tm, query, source=[])
        if s:
            return s[-1]
        query = {
            "name.keyword": name,
            "corpus": corpus,
        }
        s = search(ES_CLIENT, index_tm, query, source=[])
        if s:
            return s[-1]
    raise TMNotFoundException("Topic Modelling index not found!")


def dataset_prepare(**kwargs):
    import os
    import shutil
    import artm
    import datetime
    from elasticsearch_dsl import Search, Q
    from util.util import is_kazakh, is_latin
    from dags.bigartm.services.cleaners import return_cleaned_array, txt_writer
    from util.constants import BASE_DAG_DIR

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
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
    fast = 'fast' in kwargs
    name = kwargs['name']
    name_translit = kwargs['name_translit']
    corpus = kwargs['corpus']
    if type(corpus) != list:
        corpus = [corpus]
    corpus_datetime_ignore = kwargs.get('corpus_datetime_ignore', [])
    source = kwargs['source']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    group_id = kwargs['group_id']
    topic_weight_threshold = kwargs['topic_weight_threshold']
    topic_doc = kwargs['topic_doc']
    uniq_topic_doc = kwargs['uniq_topic_doc']
    temp_folder = kwargs['temp_folder']
    text_field = kwargs['text_field']
    is_dynamic = 'is_dynamic' in kwargs and kwargs['is_dynamic']

    # Extract
    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("terms", corpus=corpus) \
                                                        .filter('exists', field=text_field)
    q_from = Q()
    q_to = Q()
    if source:
        s = s.filter("term", **{"source": source})
    if datetime_from:
        q_from = Q("range", datetime={"gte": datetime_from})
    if datetime_to and not perform_actualize:
        q_to = Q("range", datetime={"lte": datetime_to})
    q = (q_from & q_to)
    for corpus_to_ignore in corpus_datetime_ignore:
        q = q | (~Q('exists', field="datetime") & Q("term", corpus=corpus_to_ignore))
    s = s.query(q)
    s = s.source(["id", "text", text_field, "title", "source", "num_views", "num_comments", "datetime", "corpus"])[:5000000]

    group_document_es_ids = None
    if group_id:
        group = TopicGroup.objects.get(id=group_id)
        topic_ids = [t.topic_id for t in group.topics.all()]
        if not topic_ids:
            return "Group is empty"
        topic_modelling_name = group.topic_modelling_name
        st = Search(using=ES_CLIENT, index=f"{topic_doc}_{topic_modelling_name}") \
                 .filter("terms", **{"topic_id": topic_ids}) \
                 .filter("range", topic_weight={"gte": topic_weight_threshold}) \
                 .filter("range", datetime={"gte": datetime.date(2000, 1, 1)}) \
                 .source(('document_es_id'))[:5000000]
        r = st.scan()
        group_document_es_ids = set([doc.document_es_id for doc in r])

    # Exclude document already in TM if actualizing
    ids_to_skip = None
    if perform_actualize:
        std = Search(using=ES_CLIENT, index=f"{uniq_topic_doc}_{name}").source(['document_es_id'])[:5000000]
        ids_to_skip = set((doc.document_es_id for doc in std.scan()))

    ids = []
    texts = []
    titles = []
    sources = []
    dates = []
    corpuses = []
    num_views = []
    num_comments = []
    meta_ids_in_list = set()
    ids_in_list = set()
    for document in s.scan():
        if len(document[text_field]) < 100 and "hate" not in corpus:
            continue
        if document.meta.id in meta_ids_in_list or document.id in ids_in_list:
            continue
        if ids_to_skip is not None and document.meta.id in ids_to_skip:
            continue
        if group_document_es_ids is not None and document.meta.id not in group_document_es_ids:
            continue
        if "_kz_" not in text_field and is_kazakh(document.text + (document.title if document.title else "")):
            continue
        if "_en_" not in text_field and is_latin(document.text + (document.title if document.title else "")):
            continue
        ids.append(document.meta.id)
        meta_ids_in_list.add(document.meta.id)
        ids_in_list.add(document.id)
        titles.append(document.title)
        sources.append(document.source)
        dates.append(document.datetime if hasattr(document, "datetime") and document.datetime else "")
        corpuses.append(document.corpus)
        num_views.append(document.num_views if hasattr(document, "num_views") else -1)
        num_comments.append(document.num_comments if hasattr(document, "num_comments") else -1)

        # Clean junk
        text = document[text_field]
        if "_en_" not in text_field:
            text = " ".join([w for w in text.split() if not is_latin(w, threshold=0.1)])
        texts.append(text)
    titles = return_cleaned_array(titles)

    formated_data = []
    for id, text, title, source, date, corpus_d, views, comments in zip(ids, texts, titles, sources, dates, corpuses, num_views, num_comments):
        formated_data.append(f'{id}*{source.replace(" ", "_")}*{date}*{corpus_d}*{views}*{comments}' + ' ' +
                             '|text' + ' ' + text + ' ' +
                             '|title' + ' ' + title + ' ')

    data_folder = os.path.join(BASE_DAG_DIR, temp_folder)

    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    if is_dynamic:
        data_folder = os.path.join(data_folder,
                                   f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}{'_fast' if fast else ''}_{datetime_from.date()}_{datetime_to.date()}")
    else:
        data_folder = os.path.join(data_folder,
                                   f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}{'_fast' if fast else ''}_{datetime_from}_{datetime_to}")
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
    import glob
    import os
    import logging
    import shutil

    from dags.bigartm.services.bigartm_utils import \
    (
        send_tds_to_es_wrapper,
        send_unique_ids_to_es,
        model_train
    )
    from util.constants import BASE_DAG_DIR


    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    perform_actualize = 'perform_actualize' in kwargs
    name = kwargs['name']

    topic_doc = kwargs['topic_doc']
    uniq_topic_doc = kwargs['uniq_topic_doc']
    temp_folder = kwargs['temp_folder']
    models_folder_name = kwargs['models_folder']
    is_dynamic = 'is_dynamic' in kwargs and kwargs['is_dynamic']

    name_translit = kwargs['name_translit']
    datetime_from = kwargs['datetime_from']
    datetime_to = kwargs['datetime_to']
    regularization_params = kwargs['regularization_params']
    is_actualizable = 'is_actualizable' in kwargs and kwargs['is_actualizable']
    fast = 'fast' in kwargs and kwargs['fast']
    index_tm = kwargs['index_tm']
    tm_index = get_tm_index(**kwargs)

    data_folder = os.path.join(BASE_DAG_DIR, temp_folder)
    if is_dynamic:
        data_folder = os.path.join(data_folder,
                                   f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}{'_fast' if fast else ''}_{datetime_from.date()}_{datetime_to.date()}")
    else:
        data_folder = os.path.join(data_folder,
                                   f"bigartm_formated_data_{name if not name_translit else name_translit}{'_actualize' if perform_actualize else ''}{'_fast' if fast else ''}_{datetime_from}_{datetime_to}")

    batches_folder = os.path.join(data_folder, "batches")
    if perform_actualize and not os.path.exists(batches_folder):
        return f"No documents to actualize"

    model_artm, batch_vectorizer = model_train(batches_folder, models_folder_name, perform_actualize, tm_index,
                                               regularization_params, name, name_translit, index_tm)
    theta_documents = send_tds_to_es_wrapper(model_artm, perform_actualize, tm_index, batch_vectorizer,
                                             topic_doc, name, is_dynamic, is_actualizable, index_tm)
    send_unique_ids_to_es(theta_documents, tm_index, is_dynamic, perform_actualize,
                          kwargs['to_date'] if 'to_date' in kwargs else "", datetime_to, uniq_topic_doc, name)

    # Remove logs
    fileList = glob.glob(f'{BASE_DAG_DIR}/bigartm.*')
    for filePath in fileList:
        try:
            os.remove(filePath)
        except:
            print("!Someone already deleted file")
    # Remove batches and stuff
    shutil.rmtree(data_folder, ignore_errors=True)
    return theta_documents.shape[0]
