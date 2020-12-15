def report_subscriptions(source, filename):
    import json

    from elasticsearch_dsl import Search, Q
    from pymorphy2 import MorphAnalyzer
    from stop_words import get_stop_words

    from mainapp.models_user import Subscription, SubscriptionReportObject
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_CUSTOM_DICTIONARY_WORD, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT

    # Try to report subscription
    stopwords_ru = set(get_stop_words('ru'))
    morph = MorphAnalyzer()
    ss = Search(using=ES_CLIENT, index=ES_INDEX_CUSTOM_DICTIONARY_WORD)
    r = ss[:1000000].scan()
    custom_dict = dict((w.word, w.word_normal) for w in r)

    subscriptions = Subscription.objects.filter(is_active=True, is_fast=True)
    for subscription in subscriptions:
        criterions_evals_dict = get_criterions_dict(subscription)
        tm_index = get_tm_index(subscription.topic_modelling_name)
        data_folder = get_data_folder(source)

        # with open("1.json", "r", encoding='utf-8') as f: # TODO RETURN DEBUG
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())

        texts, urls, titles, datetimes = write_batches(news, data_folder, stopwords_ru, morph, custom_dict)
        if subscription.parent_group:
            print("!!!", "Filtering parent")
            print("!!!", "Before", len(news))
            tm_index_parent = get_tm_index(subscription.parent_group.topic_modelling_name)
            theta_values, theta_topics = get_topic_weights(data_folder, tm_index_parent)
            good_indices = set()
            group_topic_ids = set((topic.topic_id for topic in subscription.parent_group.topics.all()))
            for i, weights in enumerate(theta_values):
                for weight, topic_id in zip(weights, theta_topics):
                    if topic_id not in group_topic_ids:
                        continue
                    if weight > subscription.parent_group_threshold:
                        good_indices.add(i)
            news = []
            for i in good_indices:
                news.append(
                    {
                        "text": texts[i],
                        "title": titles[i],
                        "url": urls[i],
                        "datetime": datetimes[i],
                    }
                )
            print("!!!", "After", len(news))
            texts, urls, titles, datetimes = write_batches(news, data_folder, stopwords_ru, morph, custom_dict)

        if not texts:
            return f"No documents to actualize"

        theta_values, theta_topics = get_topic_weights(data_folder, tm_index)
        output = get_output(theta_values, theta_topics, criterions_evals_dict, subscription, source, urls, titles, datetimes)

        if output:
            send_output(output, source, subscription)
        print("!!!", len(output))


def write_batches(news, data_folder, stopwords_ru, morph, custom_dict):
    import artm
    import datetime
    import os
    import re
    import shutil

    import pytz

    from dags.bigartm.services.cleaners import txt_writer
    from dags.tm_preprocessing_lemmatize.services.tm_preproc_services import morph_with_dictionary
    from util.util import is_kazakh, is_latin

    texts = []
    urls = []
    titles = []
    datetimes = []
    for new in news:
        text = new['text']
        if is_kazakh(text) or is_latin(text):
            continue
        if 'datetime' in new:
            datetime_new = datetime.datetime.strptime(new['datetime'], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=pytz.timezone('Asia/Almaty'))
            if datetime_new.date() > datetime.datetime.now().date() and datetime_new.day <= 12:
                datetime_new = datetime_new.replace(month=datetime_new.day, day=datetime_new.month)
            if (datetime.datetime.now().date() - datetime_new.date()).days > 5:
                continue
        else:
            continue
        # Preprocess text
        text = " ".join(x.lower() for x in
                        ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', text).split()).split())
        cleaned_words_list = [morph_with_dictionary(morph, word, custom_dict) for word in text.split() if
                              len(word) > 2 and word not in stopwords_ru]
        texts.append(" ".join(cleaned_words_list))
        urls.append(new['url'])
        titles.append(new['title'])
        # datetimes.append(datetime_new)
        datetimes.append(None)

    print("!!!", "Write batches")
    # Write batches
    formated_data = []
    for i, text in enumerate(texts):
        formated_data.append(f'{i}' + ' ' + '|text' + ' ' + text + ' ')

    shutil.rmtree(data_folder, ignore_errors=True)
    os.mkdir(data_folder)
    if len(formated_data) == 0:
        print("No documents")
        return [], [], [], []
    print("!!!", f"Writing {len(formated_data)} documents")

    txt_writer(data=formated_data, filename=os.path.join(data_folder, f"bigartm_formated_data.txt"))
    artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data.txt"),
                         data_format="vowpal_wabbit",
                         target_folder=os.path.join(data_folder, "batches"))
    return texts, urls, titles, datetimes


def get_topic_weights(data_folder, tm_index):
    import artm
    import os

    from dags.bigartm.services.bigartm_utils import load_monkey_patch
    from util.constants import BASE_DAG_DIR

    print("!!!", "Get topic weights")
    batches_folder = os.path.join(data_folder, "batches")
    batch_vectorizer = artm.BatchVectorizer(data_path=batches_folder,
                                            data_format='batches')
    model_folder = os.path.join(BASE_DAG_DIR, "bigartm_models")
    model_artm = artm.ARTM(num_topics=tm_index.number_of_topics,
                           class_ids={"text": 1}, theta_columns_naming="title",
                           reuse_theta=True, cache_theta=True, num_processors=4)
    model_artm.load = load_monkey_patch
    model_artm.load(model_artm, os.path.join(model_folder, f"model_{tm_index.name}.model"))

    theta = model_artm.transform(batch_vectorizer)

    theta_values = theta.values.transpose().astype(float)
    theta_topics = theta.index.array.to_numpy().astype(str)

    return theta_values, theta_topics


def get_output(theta_values, theta_topics, criterions_evals_dict, subscription, source, urls, titles, datetimes):
    from django.db import IntegrityError
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.models_user import SubscriptionReportObject

    output = []
    # Get threshold
    ss = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{subscription.topic_modelling_name}_{subscription.criterion.id}")[:0]
    ss.aggs.metric("percents", agg_type="percentiles", field="value", percents=[subscription.threshold])
    r = ss.execute()
    eval_threshold = list(r.aggregations.percents.values.__dict__['_d_'].values())[0]

    ss = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{subscription.topic_modelling_name}")[:0]
    ss.aggs.metric("percents", agg_type="percentiles", field="topic_weight", percents=[subscription.tm_weight_threshold])
    r = ss.execute()
    tm_threshold = list(r.aggregations.percents.values.__dict__['_d_'].values())[0]

    print("!!!", "Calc evals")
    for i, weights in enumerate(theta_values):
        res = 0
        relevant_count = 0
        for weight, topic_id in zip(weights, theta_topics):
            if weight >= tm_threshold:
                relevant_count += 1
            if topic_id not in criterions_evals_dict:
                continue
            res += weight * criterions_evals_dict[topic_id]
        if relevant_count < subscription.tm_num_threshold:
            continue
        if (subscription.subscription_type == -1 and res < eval_threshold) or \
                (subscription.subscription_type == 1 and res > eval_threshold) or \
                (subscription.subscription_type == 0):
            sro = SubscriptionReportObject(
                subscription=subscription,
                source=source,
                url=urls[i],
                title=titles[i],
                datetime=datetimes[i],
                value=res,
                is_sent=True,
            )
            try:
                sro.save()
            except IntegrityError as e:
                continue
            output.append(sro)
    return output


def send_output(output, source, subscription):
    from django.core.mail import send_mail

    print("!!!", "Sending")
    message = "Добрый день!\n\n" \
              "Обнаружены публикации:\n\n"
    html_message = message
    for new in output:
        message += f"{new.title}\n" \
                   f"Ссылка: {new.url}\n" \
                   f"Уровень опасности: {round(new.value, 2)}\n\n"

        html_message += f"<hr><br>" \
                        f"{new.title}<br>" \
                        f"<b>Ссылка</b>: {new.url}<br>" \
                        f"<b>Уровень опасности</b>: {round(new.value, 2)}<br><br>"
    try:
        send_mail(subject=f"Отчёт по источнику {source.name}",
                  message=message,
                  recipient_list=[subscription.user.email],
                  from_email="media.bot.kz@yandex.ru",
                  html_message=html_message
                  )
    except Exception as e:
        print("!!!", "Mail send fail", e)


def get_data_folder(source):
    import datetime
    import os

    from util.constants import BASE_DAG_DIR

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_scrap_temp")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_monitoring_{datetime.datetime.now()}_{source.id}")
    return data_folder


def get_tm_index(topic_modelling_name):
    import os

    import artm
    from elasticsearch_dsl import Search, Q
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING

    from util.constants import BASE_DAG_DIR

    from dags.bigartm.services.bigartm_utils import load_monkey_patch

    print("!!!", "Get topic model")
    ss = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING)
    ss = ss.query(Q(name=topic_modelling_name) | Q(**{"name.keyword": topic_modelling_name}))
    ss = ss.filter("term", is_ready=True)
    tm_index = ss.source(['number_of_topics', 'name']).execute()[0]

    model_artm = artm.ARTM(num_topics=tm_index.number_of_topics,
                           class_ids={"text": 1}, theta_columns_naming="title",
                           reuse_theta=True, cache_theta=True, num_processors=4)
    model_artm.load = load_monkey_patch
    model_artm.load(model_artm, os.path.join(os.path.join(BASE_DAG_DIR, "bigartm_models"), f"model_{topic_modelling_name}.model"))
    return tm_index


def get_criterions_dict(subscription):
    from evaluation.models import TopicsEval

    print("!!!", "Get topic evals")
    # Get topic evals
    evaluations = TopicsEval.objects.filter(criterion=subscription.criterion,
                                            topics__topic_modelling_name=subscription.topic_modelling_name)\
        .distinct().prefetch_related('topics')

    # Topic -> [List of evaluations by each author]
    criterions_evals_dict = {}
    for evaluation in evaluations:
        if not evaluation.topics.exists():
            continue
        eval_topic_id = evaluation.topics.first().topic_id
        if eval_topic_id not in criterions_evals_dict:
            criterions_evals_dict[eval_topic_id] = []
        criterions_evals_dict[eval_topic_id].append(evaluation.value)

    for t in criterions_evals_dict.keys():
        criterions_evals_dict[t] = sum(criterions_evals_dict[t]) / len(criterions_evals_dict[t])
    return criterions_evals_dict
