def scrap(**kwargs):
    import os
    import subprocess
    import datetime
    import json
    import pytz

    from util.constants import BASE_DAG_DIR
    from django.db import IntegrityError

    from mainapp.models import ScrapRules, Document, Source, Author

    # Init
    source_url = kwargs['source_url']
    source_id = kwargs['source_id']
    perform_full = kwargs['perform_full']
    perform_fast = kwargs['perform_fast']
    source = Source.objects.get(id=source_id)
    rules = ScrapRules.objects.filter(source=source)
    if not rules.exists() or not rules.filter(type=1).exists():
        return "No rules - no parse"

    # Scrap
    os.chdir(os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project"))
    safe_source_url = source_url.replace('https://', '').replace('http://', '').replace('/', '')
    filename = f"{safe_source_url}_{str(datetime.datetime.now()).replace(':', '-')}.json"
    run_args = ["scrapy", "crawl", "spider", "-o", filename]
    for rule in rules:
        run_args.append("-a")
        run_args.append(f"{dict(ScrapRules.TYPES)[rule.type]}={rule.selector}")
    run_args.append("-a")
    run_args.append(f"url={source_url}")
    ds_w_date = Document.objects.exclude(datetime=None).filter(source__id=source_id)
    latest_date = None
    if ds_w_date.exists():
        latest_date = ds_w_date.latest('datetime').datetime
    if not latest_date:
        latest_date = datetime.datetime.now() - datetime.timedelta(days=365)
    else:
        latest_date -= datetime.timedelta(days=7)
    run_args.append("-a")
    run_args.append(f"latest_date={latest_date.isoformat()}")
    run_args.append("-a")
    run_args.append(f"perform_full={'yes' if perform_full else 'no'}")
    if perform_fast:
        run_args.append("-a")
        run_args.append(f"max_depth=3")
    if perform_full:
        spider_timeout = 0
    elif perform_fast:
        spider_timeout = 90 * 60
    else:
        spider_timeout = 33 * 60 * 60
    run_args.append("-s")
    run_args.append(f"CLOSESPIDER_TIMEOUT={spider_timeout}")
    try:
        print(f"Run command: {run_args}")
    except:
        pass
    subprocess.run(run_args)

    # Report subscriptions
    report_subscriptions(source, filename)

    # Write to DB
    filename = os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project", filename)
    new_news = 0
    try:
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())
            for new in news:
                # if is_kazakh(new['text'] + new['title']) or is_latin(new['text'] + new['title']):
                #     continue
                new['source'] = source
                if 'title' in new:
                    new['title'] = new['title'][:Document._meta.get_field('title').max_length]
                if 'author' in new:
                    new['author'] = new['author'][:Author._meta.get_field('name').max_length]
                    if Author.objects.filter(name=new['author']).exists():
                        new['author'] = Author.objects.get(name=new['author'], corpus=source.corpus)
                    else:
                        new['author'] = Author.objects.create(name=new['author'], corpus=source.corpus)
                if 'datetime' in new:
                    new['datetime'] = datetime.datetime.strptime(new['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Almaty'))
                    if new['datetime'].date() > datetime.datetime.now().date() and new['datetime'].day <= 12:
                        new['datetime'] = new['datetime'].replace(month=new['datetime'].day, day=new['datetime'].month)
                try:
                    Document.objects.create(**new)
                    new_news += 1
                except IntegrityError:
                    pass
            if len(news) <= 3:
                raise Exception("Seems like parser is broken - less than 3 news")
    finally:
        os.remove(filename)
    return f"Parse complete, {new_news} parsed"


def report_subscriptions(source, filename):
    import artm
    import datetime
    import json
    import shutil
    import os
    import re
    import pytz

    from django.core.mail import send_mail
    from django.db import IntegrityError
    from elasticsearch_dsl import Search, Q
    from pymorphy2 import MorphAnalyzer
    from stop_words import get_stop_words

    from dags.bigartm.services.bigartm_utils import load_monkey_patch
    from dags.bigartm.services.cleaners import txt_writer
    from dags.tm_preprocessing_lemmatize.services.tm_preproc_services import morph_with_dictionary
    from util.constants import BASE_DAG_DIR
    from util.util import is_kazakh, is_latin

    from evaluation.models import TopicsEval
    from mainapp.models_user import Subscription, SubscriptionReportObject
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_CUSTOM_DICTIONARY_WORD, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT

    # Try to report subscription
    subscriptions = Subscription.objects.filter(is_active=True, is_fast=True)
    for s in subscriptions:
        # Get topic model
        print("!!!", "Get topic model")
        ss = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING)
        ss = ss.query(Q(name=s.topic_modelling_name) | Q(**{"name.keyword": s.topic_modelling_name}))
        ss = ss.filter("term", is_ready=True)
        tm_index = ss.source(['number_of_topics', 'name']).execute()[0]
        model_artm = artm.ARTM(num_topics=tm_index.number_of_topics,
                               class_ids={"text": 1}, theta_columns_naming="title",
                               reuse_theta=True, cache_theta=True, num_processors=4)
        model_artm.load = load_monkey_patch
        model_artm.load(model_artm, os.path.join(os.path.join(BASE_DAG_DIR, "bigartm_models"), f"model_{s.topic_modelling_name}.model"))

        print("!!!", "Get topic evals")
        # Get topic evals
        evaluations = TopicsEval.objects.filter(criterion=s.criterion, topics__topic_modelling_name=s.topic_modelling_name).distinct().prefetch_related('topics')

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

        print("!!!", "Preprocess")
        # Prepare lemmatizers
        stopwords_ru = set(get_stop_words('ru'))
        morph = MorphAnalyzer()
        ss = Search(using=ES_CLIENT, index=ES_INDEX_CUSTOM_DICTIONARY_WORD)
        r = ss[:1000000].scan()
        custom_dict = dict((w.word, w.word_normal) for w in r)

        output = []
        # with open("1.json", "r", encoding='utf-8') as f: # TODO RETURN DEBUG
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())
            texts = []
            urls = []
            titles = []
            datetimes = []
            for new in news:
                text = new['text']
                if is_kazakh(text) or is_latin(text):
                    continue
                if 'datetime' in new:
                    datetime_new = datetime.datetime.strptime(new['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Almaty'))
                    if datetime_new.date() > datetime.datetime.now().date() and datetime_new.day <= 12:
                        datetime_new = datetime_new.replace(month=datetime_new.day, day=datetime_new.month)
                    if (datetime.datetime.now().date() - datetime_new.date()).days > 5:
                        continue
                else:
                    continue
                # Preprocess text
                text = " ".join(x.lower() for x in ' '.join(re.sub('([^А-Яа-яa-zA-ZӘәҒғҚқҢңӨөҰұҮүІі-]|[^ ]*[*][^ ]*)', ' ', text).split()).split())
                cleaned_words_list = [morph_with_dictionary(morph, word, custom_dict) for word in text.split() if len(word) > 2 and word not in stopwords_ru]
                texts.append(" ".join(cleaned_words_list))
                urls.append(new['url'])
                titles.append(new['title'])
                datetimes.append(datetime_new)

            print("!!!", "Write batches")
            # Write batches
            formated_data = []
            for i, text in enumerate(texts):
                formated_data.append(f'{i}' + ' ' + '|text' + ' ' + text + ' ')

            data_folder = os.path.join(BASE_DAG_DIR, "bigartm_scrap_temp")
            if not os.path.exists(data_folder):
                os.mkdir(data_folder)

            data_folder = os.path.join(data_folder, f"bigartm_formated_data_monitoring_{datetime.datetime.now()}_{source.id}")
            shutil.rmtree(data_folder, ignore_errors=True)
            os.mkdir(data_folder)
            if len(formated_data) == 0:
                return f"No documents to actualize"
            print("!!!", f"Writing {len(formated_data)} documents")

            txt_writer(data=formated_data, filename=os.path.join(data_folder, f"bigartm_formated_data.txt"))
            artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data.txt"),
                                 data_format="vowpal_wabbit",
                                 target_folder=os.path.join(data_folder, "batches"))

            # Get topic weights
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

            # Get threshold
            ss = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{s.topic_modelling_name}_{s.criterion.id}")[:0]
            ss.aggs.metric("percents", agg_type="percentiles", field="value", percents=[s.threshold])
            r = ss.execute()
            eval_threshold = list(r.aggregations.percents.values.__dict__['_d_'].values())[0]

            ss = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{s.topic_modelling_name}")[:0]
            ss.aggs.metric("percents", agg_type="percentiles", field="topic_weight", percents=[s.tm_weight_threshold])
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
                if relevant_count < s.tm_num_threshold:
                    continue
                if (s.subscription_type == -1 and res < eval_threshold) or \
                   (s.subscription_type == 1 and res > eval_threshold) or \
                   (s.subscription_type == 0):
                    sro = SubscriptionReportObject(
                        subscription=s,
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
        if output:
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
                          recipient_list=[s.user.email],
                          from_email="media.bot.kz@yandex.ru",
                          html_message=html_message
                          )
            except Exception as e:
                print("!!!", "Mail send fail", e)
