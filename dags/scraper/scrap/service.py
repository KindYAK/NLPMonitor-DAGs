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
    import json
    import os

    from elasticsearch_dsl import Search, Q

    from dags.bigartm.services.bigartm_utils import load_monkey_patch
    from util.constants import BASE_DAG_DIR

    from evaluation.models import TopicsEval
    from mainapp.models_user import Subscription, SubscriptionReportObject
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING

    # Try to report subscription
    ss = Subscription.objects.filter(is_active=True, is_fast=True)
    for s in ss:
        # Get topic model
        ss = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING)
        ss = ss.query(Q(name=s.topic_modelling_name) | Q(**{"name.keyword": s.topic_modelling_name}))
        ss = ss.filter("term", is_ready=True)
        tm_index = ss.execute().source(['number_of_topics'])[0]
        model_artm = artm.ARTM(num_topics=tm_index.number_of_topics,
                               class_ids={"text": 1}, theta_columns_naming="title",
                               reuse_theta=True, cache_theta=True, num_processors=4)
        model_artm.load = load_monkey_patch
        model_artm.load(model_artm, os.path.join(os.path.join(BASE_DAG_DIR, "bigartm_models"), f"model_{s.topic_modelling_name}.model"))

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

        output = []
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())
            for new in news:
                text = new['text']
                url = new['url']
                # Preprocess text TODO

                # Get topic weights TODO

                # Get eval TODO

                # Add to output TODO
                if True:
                    sro = SubscriptionReportObject(
                        subscription=s,
                        source=source,
                        url=url,
                        is_sent=True,
                    )
                    output.append(sro)
        if output:
            # Send output TODO
            pass