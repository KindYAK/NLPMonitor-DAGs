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
            s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)\
                .source(['datetime', 'source', 'text', 'text_lemmatized', 'title', 'text_lemmatized_yandex'])
            q = Q(
                'bool',
                should=[Q("match_phrase", text_lemmatized=geo.name)] +
                       [Q("match_phrase", text=geo.name)] +
                       [Q("match_phrase", title=geo.name)] +
                       [Q("match_phrase", text_lemmatized_yandex=geo.name)],
                minimum_should_match=1,
            )
            s = s.query(q)
            s = s.extra(track_scores=True)
            print(f'!!! Scans count for {i} geo inside place: ', s.count(), datetime.datetime.now())
            scans = s.scan()

            for scan_obj in scans:

                document_datetime, document_source = hit_parser(scan_obj)

                doc = DocumentLocation(
                    document_es_id=scan_obj.meta.id,
                    document_datetime=document_datetime,
                    document_source=document_source,
                    location_name=geo.name,
                    location_level=location_level,
                    location_weight=scan_obj.meta.score,
                    location_id=geo.id,
                )

                for tm, criterion_id in criterion_tm_duos:
                    ev_docs = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{tm}_{criterion_id}") \
                        .filter("term", document_es_id=scan_obj.meta.id) \
                        .source(['value', 'document_datetime', 'document_source']) \
                        .execute()

                    if not ev_docs:
                        continue

                    ev_docs = ev_docs[0]

                    value = ev_docs.value if hasattr(ev_docs, "value") and ev_docs.value else None

                    doc[f'criterion_{tm}_{criterion_id}'] = value

                yield doc


def hit_parser(row):
    document_datetime = row.datetime if hasattr(row, "datetime") and row.datetime else None
    document_source = row.source if hasattr(row, "source") and row.source else None
    return document_datetime, document_source
