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


def score_docs(**kwargs):
    import os
    import re
    import shutil

    import artm
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_CUSTOM_DICTIONARY_WORD
    from django.contrib.auth.models import User
    from evaluation.models import EvalCriterion, TopicsEval, TopicIDEval
    from pymorphy2 import MorphAnalyzer
    from stop_words import get_stop_words
    from sklearn.preprocessing import MinMaxScaler

    from dags.bigartm.services.cleaners import txt_writer
    from dags.tm_preprocessing.services.tm_preproc_services import morph_with_dictionary
    from util.constants import BASE_DAG_DIR
    from mainapp.models_user import TopicID
    import numpy as np

    from annoying.functions import get_object_or_None

    num_topics = kwargs['num_topics']
    model_name = kwargs['model_name']
    criterion_name = kwargs['criterion_name']
    criterion_name_unicode = kwargs['criterion_name_unicode']
    docs_folder_name = kwargs['docs_folder_name']

    models_folder_name = 'bigartm_models'
    temp_folder = 'bigartm_temp'
    model_folder = os.path.join(BASE_DAG_DIR, models_folder_name)
    model_artm = artm.ARTM(
        num_topics=num_topics,
        class_ids={"text": 1},
        theta_columns_naming="title",
        reuse_theta=True,
        cache_theta=True,
        num_processors=4
    )

    model_artm.load = load
    model_artm.load(model_artm, os.path.join(model_folder, f"model_{model_name}.model"))
    data_folder = os.path.join(BASE_DAG_DIR, temp_folder)

    stopwords = get_stop_words('ru')
    morph = MorphAnalyzer()

    scaler = MinMaxScaler(feature_range=(0, 1))

    s = Search(using=ES_CLIENT, index=ES_INDEX_CUSTOM_DICTIONARY_WORD)
    r = s[:1000000].scan()
    custom_dict = dict((w.word, w.word_normal) for w in r)
    formatted_data = []
    for i, file in enumerate(os.listdir(os.path.join(BASE_DAG_DIR, docs_folder_name))):
        with open(os.path.join(BASE_DAG_DIR, docs_folder_name, file), 'r', encoding='utf-8') as f:
            file_to_process = f.read()

        cleaned_doc = " ".join(x.lower() for x in ' '.join(
            re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', file_to_process).split()).split())
        cleaned_words_list = [morph_with_dictionary(morph, word, custom_dict) for word in cleaned_doc.split() if
                              len(word) > 2 and word not in stopwords]
        cleaned_words_list = [word for word in cleaned_words_list if len(word) > 2]
        cleaned_doc = " ".join(cleaned_words_list)
        formatted_data.append(f'{i} |text' + ' ' + cleaned_doc)

    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{criterion_name}")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    txt_writer(data=formatted_data,
               filename=os.path.join(data_folder, f"bigartm_formated_data_{criterion_name}.txt"))

    artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data_{criterion_name}.txt"),
                         data_format="vowpal_wabbit",
                         target_folder=os.path.join(data_folder, "batches"))

    batches_folder = os.path.join(data_folder, "batches")
    batch_vectorizer = artm.BatchVectorizer(data_path=batches_folder,
                                            data_format='batches')

    theta = model_artm.transform(batch_vectorizer)

    author = User.objects.filter(is_superuser=True).first()

    criterion = get_object_or_None(EvalCriterion, name=criterion_name_unicode)
    topic_modelling = model_name
    if criterion:
        criterion_id = criterion.id
    else:
        criterion_id = EvalCriterion.objects.create(name=criterion_name_unicode).id

    print('!!! theta', theta, type(theta))
    print('!!! mean of not null topics', theta[theta != 0].mean(axis=1))
    value_list = []
    for row in theta.itertuples():
        row_sum = 0
        not_zero_cnt = 0
        for val in row[1:]:
            if val:
                row_sum += val
                not_zero_cnt += 1
        try:
            value = row_sum / not_zero_cnt
        except ZeroDivisionError:
            value = 0
        value_list.append(value)

    value_list = scaler.fit_transform(np.array(value_list).reshape(-1, 1))

    for i, row in enumerate(theta.itertuples()):
        topic_id_obj = get_object_or_None(TopicID, topic_modelling_name=topic_modelling, topic_id=row[0])
        if not topic_id_obj:
            topic_id_obj = TopicID.objects.create(topic_modelling_name=topic_modelling, topic_id=row[0])
        topics_eval = get_object_or_None(TopicsEval, topics=topic_id_obj, topicideval__weight=1,
                                         criterion__id=criterion_id,
                                         author=author)
        if topics_eval:
            topics_eval.value = value_list[i]
            topics_eval.save()
        else:
            topics_eval = TopicsEval.objects.create(criterion_id=criterion_id, value=value_list[i], author=author)
            TopicIDEval.objects.create(
                topic_id=topic_id_obj,
                topics_eval=topics_eval,
                weight=1
            )

    shutil.rmtree(data_folder, ignore_errors=True)
