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

                    document_datetime, value, document_source = hit_parser(ev_docs)

                    yield DocumentLocation(
                        document_es_id=scan_obj.meta.id,
                        document_datetime=document_datetime,
                        document_source=document_source,
                        location_name=geo.name,
                        location_level=location_level,
                        criterion_value=value,
                        location_weight=scan_obj.meta.score,
                        topic_modelling=tm,
                        location_id=geo.id,
                        criterion_id=criterion_id
                    )


def hit_parser(row):
    document_datetime = row.document_datetime if hasattr(row, "document_datetime") and row.document_datetime else None
    value = row.value if hasattr(row, "value") and row.value else None
    document_source = row.document_source if hasattr(row, "document_source") and row.document_source else None
    return document_datetime, value, document_source
