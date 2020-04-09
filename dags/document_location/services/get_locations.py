def get_locations(**kwargs):
    from collections import defaultdict

    from geo.models import Area
    from eval.models import EvalCriterion
    from mainapp.documents import DocumentLocation
    from mainapp.constants import SEARCH_CUTOFF_CONFIG

    from elasticsearch_dsl import Search, Q

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, \
        ES_INDEX_TOPIC_DOCUMENT

    topic_modelling = kwargs['topic_modelling']
    perform_actualize = 'perform_actualize' in kwargs
    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])
    # TODO как кошерно заюзать criterion
    doc_locations = list()
    for places in (Area):  # , District, Locality):
        location_level = places.objects.first()._meta.verbose_name
        for geo in places.objects.all():
            s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).source(['text', 'text_lemmatized', 'title'])
            Q(
                'bool',
                should=[Q("match_phrase", text_lemmatized=geo.name)] +
                       [Q("match_phrase", text=geo.name)] +
                       [Q("match_phrase", title=geo.name)],
                minimum_should_match=1,
            )
            s = s.query(Q)
            s = s[:SEARCH_CUTOFF_CONFIG["SEARCH_LVL_HARD"]['ABS_MAX_RESULTS_CUTOFF']]
            documents_es_id = s.execute()

            for document_es_id in documents_es_id:
                topic_doc = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}") \
                    .filter("range", topic_weight={"gte": 0.001}).filter("term", document_es_id=document_es_id) \
                    .source(['topic_weight', "datetime", "document_source"])
                data = defaultdict(int)
                # TODO почему тут просто execute
                for td in topic_doc.execute():
                    data["datetime"] = td.datetime
                    # TODO кошерно считать этот location weight
                    data["eval_value"] += td.topic_weight
                    data["source"] = td.document_source
                    data["total_topic_weight"] += 1

                doc_locations.append(
                    DocumentLocation(
                        document_es_id=document_es_id,
                        document_datetime=data["datetime"],
                        document_source=data["source"],
                        location_name=geo.name,
                        location_level=location_level,
                        criterion=criterion,
                        location_weight=data["eval_value"] / data["total_topic_weight"]
                    )
                )

    # TODO parallel bulk
