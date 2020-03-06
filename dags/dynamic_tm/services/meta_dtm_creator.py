def generate_meta_dtm(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_META_DTM
    from mainapp.documents import META_DTM
    from util.service_es import search
    from elasticsearch_dsl import Index

    meta_name = kwargs['meta_dtm_name']
    volume_days = kwargs['tm_volume_days']
    delta_days = kwargs['delta_days']
    reset_index = kwargs['reset_index']
    from_date = kwargs['from_date']
    to_date = kwargs['to_date']

    if reset_index:
        Index(ES_INDEX_META_DTM).delete(using=ES_CLIENT, ignore=404)

    if not ES_CLIENT.indices.exists(ES_INDEX_META_DTM):
        ES_CLIENT.indices.create(index=ES_INDEX_META_DTM, body={
            "settings": META_DTM.Index.settings,
            "mappings": META_DTM.Index.mappings
        })

    s = search(client=ES_CLIENT, index=ES_INDEX_META_DTM,
               query={'meta_name': meta_name, 'volume_days': volume_days, 'delta_days': delta_days,
                      'from_date': from_date, 'to_date': to_date})

    if s:
        ES_CLIENT.update(index=ES_INDEX_META_DTM, id=s[-1].meta.id,
                         body=
                         {"doc":
                             {
                                 "meta_name": meta_name,
                                 "volume_days": volume_days,
                                 "delta_days": delta_days,
                                 'from_date': from_date,
                                 'to_date': to_date,
                                 'reset_index': reset_index
                             }
                         }
                         )
    else:
        index = META_DTM(**{"meta_name": meta_name,
                            "volume_days": volume_days,
                            "delta_days": delta_days,
                            'from_date': from_date,
                            'to_date': to_date,
                            'reset_index': reset_index})
        index.save()

    return 'META DTM GENERATED'
