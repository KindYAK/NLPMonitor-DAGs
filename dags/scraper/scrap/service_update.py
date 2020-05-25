def update(**kwargs):
    import datetime
    import json
    import os
    import subprocess
    from collections import defaultdict

    from elasticsearch_dsl import Search
    from django.db.models import Q, F, ExpressionWrapper, fields, Value
    from django.utils import timezone
    from mainapp.models import ScrapRules, Document, Source, Author
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    from util.constants import BASE_DAG_DIR

    # Init
    start = kwargs['start']
    end = kwargs['end']
    now = timezone.now()

    print("!!!", "Getting documents URLs", datetime.datetime.now())
    sources = Source.objects.filter(Q(scraprules__type=6) | Q(scraprules__type=8)).only('id') # Has rules for num_views or num_comments
    scrap_rules = ScrapRules.objects.filter(source__in=sources, type__in=[6, 8])
    scrap_rules_dict = defaultdict(defaultdict)
    for rule in scrap_rules:
        scrap_rules_dict[rule.source.id][rule.type] = rule.selector
    qs = Document.objects.filter(source__in=sources).only('id', 'num_views', 'num_comments', 'url',
                                                          'datetime', 'datetime_created', 'datetime_activity_parsed')
    qs = qs.exclude(url=None)
    qs = qs.annotate(
        days_since_publication=ExpressionWrapper(
            Value(now, fields.DateTimeField()) - F('datetime'),
            output_field=fields.DurationField()
        )
    )
    qs = qs.annotate(
        days_update_to_publication=ExpressionWrapper(
            F('datetime_activity_parsed') - F('datetime'),
            output_field=fields.DurationField()
        )
    )
    qg7 = Q(days_since_publication__gte=datetime.timedelta(days=7))
    ql7 = Q(days_update_to_publication__lte=datetime.timedelta(days=7))
    qg30 = Q(days_since_publication__gte=datetime.timedelta(days=30))
    ql30 = Q(days_update_to_publication__lte=datetime.timedelta(days=30))
    qs = qs.filter((qg7 & ql7) | (qg30 & ql30))
    number_of_documents = qs.count()
    if number_of_documents == 0:
        return "Nothing to update"
    qs = qs[int(start / 100 * number_of_documents):int(end / 100 * number_of_documents) + 1]
    print("!!!", number_of_documents, "to process", datetime.datetime.now())

    filename_suffix = f"{now.date()}_{start}_{end}"
    url_filename = f"url_list_update_{filename_suffix}.txt"
    if not os.path.exists(os.path.join(BASE_DAG_DIR, "tmp")):
        os.mkdir(os.path.join(BASE_DAG_DIR, "tmp"))
    url_path = os.path.join(BASE_DAG_DIR, "tmp", url_filename)
    with open(url_path, "w", encoding='utf-8') as f:
        for doc in qs:
            f.write(f"{doc.id}\n"
                    f"{doc.url}\n"
                    f"{scrap_rules_dict[doc.source.id].get(6, None)}\n"
                    f"{scrap_rules_dict[doc.source.id].get(8, None)}\n")

    # Scrap
    print("!!!", "Start scraping", datetime.datetime.now())
    os.chdir(os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project"))
    filename = f"activity_update_{filename_suffix}.json"
    run_args = ["scrapy", "crawl", "activity_update_spider", "-o", filename]
    run_args.append("-a")
    run_args.append(f"url_filename={url_path}")
    run_args.append("-s")
    run_args.append(f"CLOSESPIDER_TIMEOUT={24*60*60}")
    try:
        print(f"Run command: {run_args}")
    except:
        pass
    subprocess.run(run_args)

    # Write to DB
    print("!!!", "Writing to DB", datetime.datetime.now())
    filename = os.path.join(BASE_DAG_DIR, "dags", "scraper", "scrapy_project", filename)
    news_updated = 0
    news_skipped = 0
    to_udpate_es = []
    try:
        with open(filename, "r", encoding='utf-8') as f:
            news = json.loads(f.read())
        news_update_dict = dict(
            (new['id'],
             {
                 "num_views": new.get('num_views', None),
                 "num_comments": new.get('num_comments', None),
             }) for new in news
        )
        for doc in qs:
            if doc.id not in news_update_dict:
                news_skipped += 1
                continue
            doc.datetime_activity_parsed = now
            is_updated = False
            if news_update_dict[doc.id]['num_views'] is not None:
                is_updated = True
                doc.num_views = news_update_dict[doc.id]['num_views']
            if news_update_dict[doc.id]['num_comments'] is not None:
                is_updated = True
                doc.num_comments = news_update_dict[doc.id]['num_comments']
            if is_updated:
                to_udpate_es.append({
                    "id": doc.id,
                    "num_views": news_update_dict[doc.id]['num_views'],
                    "num_comments": news_update_dict[doc.id]['num_comments'],
                })
                news_updated += 1
            if news_updated % 1000 == 0:
                print(f"Updated {news_updated}, currently id={doc.id} new")
        Document.objects.bulk_update(qs, fields=['datetime_activity_parsed', 'num_views', 'num_comments'])
    except Exception as e:
        print("!!!!!!", "Updating DB exception", e)
    finally:
        os.remove(filename)
        os.remove(url_path)

    # Write to ES
    print("!!!", "Writing to ES", datetime.datetime.now())
    for i, doc in enumerate(to_udpate_es):
        update_body = {}
        if doc['num_views'] is not None:
            update_body['num_views'] = doc['num_views']
        if doc['num_comments'] is not None:
            update_body['num_comments'] = doc['num_comments']
        s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).filter("term", id=str(doc['id']))
        _ids = (hit.meta.id for hit in s.execute())
        for _id in _ids:
            ES_CLIENT.update(index=ES_INDEX_DOCUMENT,
                             id=_id,
                             body={"doc": update_body}
                             )
        if i % 1000 == 0:
            print(f"Writing {i}, current id={doc['id']}")
    return f"Parse complete, {news_updated} updated, {news_skipped} skipped"
