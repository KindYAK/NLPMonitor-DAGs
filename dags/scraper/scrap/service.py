def is_kazakh(text):
    return sum([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in text]) > 0.04


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
    source = Source.objects.get(id=source_id)
    rules = ScrapRules.objects.filter(source=source)
    if not rules.exists() or not rules.filter(type=1).exists():
        return "No rules - no parse"

    # Scrap
    os.chdir("/opt/bitnami/airflow/dags/dags/scraper/scrapy_project/")
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
        latest_date -= datetime.timedelta(days=30)
    run_args.append("-a")
    run_args.append(f"latest_date={latest_date.isoformat()}")
    subprocess.run(run_args)

    # Write to DB
    filename = os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project", filename)
    try:
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())
            for new in news:
                if is_kazakh(new['text'] + new['title']):
                    continue
                new['source'] = source
                if 'author' in new:
                    new['author'] = new['author'][:Author._meta.get_field('name').max_length]
                    if Author.objects.filter(name=new['author']).exists():
                        new['author'] = Author.objects.get(name=new['author'], corpus=source.corpus)
                    else:
                        new['author'] = Author.objects.create(name=new['author'], corpus=source.corpus)
                if 'datetime' in new:
                    new['datetime'] = datetime.datetime.strptime(new['datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Almaty'))
                try:
                    Document.objects.create(**new)
                except IntegrityError:
                    pass
            if len(news) <= 3:
                raise Exception("Seems like parser is broken - less than 3 news")
    finally:
        os.remove(filename)
    return "Parse complete"
