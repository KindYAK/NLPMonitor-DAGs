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
    source_id = kwargs['source_id']
    source = Source.objects.get(id=source_id)
    rules = ScrapRules.objects.filter(source=source)
    if not rules.exists() or not rules.filter(type=1).exists():
        return "No rules - no parse"

    # Scrap
    os.chdir(os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project"))
    safe_source_name = source.name.replace('https://', '').replace('http://', '').replace('/', '')
    filename = f"{source_id}_{str(datetime.datetime.now()).replace(':', '-')}.json"
    run_args = ["scrapy", "crawl", "per_url_spider", "-o", filename]
    for rule in rules:
        run_args.append("-a")
        run_args.append(f"{dict(ScrapRules.TYPES)[rule.type]}={rule.selector}")
    url_path = os.path.join(BASE_DAG_DIR, "tmp", f"urls_{source_id}.txt")
    run_args.append("-a")
    run_args.append(f"url_filename={url_path}")
    spider_timeout = 24 * 60 * 60
    run_args.append("-s")
    run_args.append(f"CLOSESPIDER_TIMEOUT={spider_timeout}")
    try:
        print(f"Run command: {run_args}")
    except:
        pass
    subprocess.run(run_args)

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
