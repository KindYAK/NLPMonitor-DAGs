def locations_generator(**kwargs):
    import datetime

    from geo.models import Area, District, Locality
    from mainapp.documents import DocumentLocation
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL

    from elasticsearch_dsl import Search, Q

    criterion_tm_duos = kwargs['criterion_tm_duos']  # ((tm_1, criterion_id_1)....()...())

    for places in (Area, District, Locality):
        location_level = places.objects.first()._meta.verbose_name
        if places == Area:
            print('!!! Parsing Areas ...', datetime.datetime.now())
        if places == District:
            print('!!! Parsing Districts ...', datetime.datetime.now())
        else:
            print('!!! Parsing Localities ...', datetime.datetime.now())
        for i, geo in enumerate(places.objects.all()):
            s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).source(['text', 'text_lemmatized', 'title'])
            q = Q(
                'bool',
                should=[Q("match_phrase", text_lemmatized=geo.name)] +
                       [Q("match_phrase", text=geo.name)] +
                       [Q("match_phrase", title=geo.name)],
                minimum_should_match=1,
            )
            s = s.query(q)
            s = s.extra(track_scores=True)
            print(f'!!! Scans count for {i} geo inside place: ', s.count(), datetime.datetime.now())
            scans = s.scan()

            for scan_obj in scans:
                for tm, criterion_id in criterion_tm_duos:
                    ev_docs = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{tm}_{criterion_id}") \
                        .filter("term", document_es_id=scan_obj.meta.id) \
                        .source(['value', 'document_datetime', 'document_source']) \
                        .execute()

                    if not ev_docs:
                        continue

                    ev_docs = ev_docs[0]

                    document_datetime = ev_docs.document_datetime if hasattr(ev_docs, "document_datetime") and ev_docs.document_datetime else None
                    value = ev_docs.value if hasattr(ev_docs, "value") and ev_docs.value else None

                    yield DocumentLocation(
                        document_es_id=scan_obj.meta.id,
                        document_datetime=document_datetime,
                        document_source=document_datetime,
                        location_name=geo.name,
                        location_level=location_level,
                        criterion_value=value,
                        location_weight=scan_obj.meta.score,
                        topic_modelling=tm,
                        location_id=geo.id,
                    )


def get_locations(**kwargs):
    from elasticsearch.helpers import parallel_bulk
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT_LOCATION

    import datetime

    failed = 0
    success = 0

    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in locations_generator(**kwargs)),
                                    index=ES_INDEX_DOCUMENT_LOCATION,
                                    chunk_size=10000, raise_on_error=True, thread_count=4):
        if (failed + success) % 10000 == 0:
            print(f"!!!{failed + success} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1

    return 'Done'
