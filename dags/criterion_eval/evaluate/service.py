def evaluate(**kwargs):
    import datetime

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT, ES_HOST
    from mainapp.documents import DocumentEval

    from elasticsearch_dsl import Search, Index
    from elasticsearch.helpers import parallel_bulk

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    print("!!!", "Forming topic-eval dict", datetime.datetime.now())
    topic_modelling = kwargs['topic_modelling']
    perform_actualize = 'perform_actualize' in kwargs
    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])
    evaluations = TopicsEval.objects.filter(criterion=criterion, topics__topic_modelling_name=topic_modelling).distinct().prefetch_related('topics')

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

    print("!!!", "Forming doc-eval dict", datetime.datetime.now())
    # Eval documents
    # Dict Document -> [topic_weight*topic_eval for ...]
    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}")
    std = std.filter("range", topic_weight={"gte": 0.001}) \
              .source(['document_es_id', 'topic_weight', 'topic_id', "datetime", "document_source"])

    ids_to_skip = None
    if perform_actualize:
        print("!!!", "Performing actualizing, skipping document already in TM")
        if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}"):
            return "Evaluation doesn't exist yet"
        s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}").source([])[:0]
        s.aggs.bucket(name="ids", agg_type="terms", field="document_es_id", size=5000000)
        r = s.execute()
        ids_to_skip = set([bucket.key for bucket in r.aggregations.ids.buckets])

    if criterion.value_range_from < 0:
        range_center = (criterion.value_range_from + criterion.value_range_to) / 2
    else:
        range_center = 0
    neutral_neighborhood = 0.1
    documents_criterion_dict = {}
    for td in std.scan():
        if ids_to_skip is not None and td.document_es_id in ids_to_skip:
            continue
        if td.topic_id not in criterions_evals_dict:
            criterion_value = 0
        else:
            criterion_value = criterions_evals_dict[td.topic_id]
        if td.document_es_id not in documents_criterion_dict:
            documents_criterion_dict[td.document_es_id] = {
                "value": [],
                "document_datetime": td.datetime if hasattr(td, "datetime") and td.datetime else None,
                "document_source": td.document_source,
                "eval_value": 0,
                "total_topic_weight": 0,
                "topic_ids_top": [],
                "topic_ids_bottom": [],
            }
        eval = td.topic_weight * criterion_value
        documents_criterion_dict[td.document_es_id]["eval_value"] += eval
        documents_criterion_dict[td.document_es_id]["total_topic_weight"] += td.topic_weight

        # Top docs
        if (any((eval > t['eval'] for t in documents_criterion_dict[td.document_es_id]['topic_ids_top']))
                or len(documents_criterion_dict[td.document_es_id]['topic_ids_top']) < 3) and \
                eval > range_center + neutral_neighborhood:
            documents_criterion_dict[td.document_es_id]['topic_ids_top'].append(
                {
                    "topic_id": td.topic_id,
                    "eval": eval
                }
            )
        if len(documents_criterion_dict[td.document_es_id]['topic_ids_top']) > 3:
            del documents_criterion_dict[td.document_es_id]['topic_ids_top'][
                documents_criterion_dict[td.document_es_id]['topic_ids_top'].index(
                    min(documents_criterion_dict[td.document_es_id]['topic_ids_top'], key=lambda x: x['eval'])
                )
            ]

        # Bottom docs
        if (any((eval < t['eval'] for t in documents_criterion_dict[td.document_es_id]['topic_ids_bottom']))
                or len(documents_criterion_dict[td.document_es_id]['topic_ids_bottom']) < 3) and \
                eval < range_center - neutral_neighborhood:
            documents_criterion_dict[td.document_es_id]['topic_ids_bottom'].append(
                {
                    "topic_id": td.topic_id,
                    "eval": eval
                }
            )
        if len(documents_criterion_dict[td.document_es_id]['topic_ids_bottom']) > 3:
            del documents_criterion_dict[td.document_es_id]['topic_ids_bottom'][
                documents_criterion_dict[td.document_es_id]['topic_ids_bottom'].index(
                    max(documents_criterion_dict[td.document_es_id]['topic_ids_bottom'], key=lambda x: x['eval'])
                )
            ]

    if perform_actualize and len(documents_criterion_dict.keys()) == 0:
        return f"No documents to actualize"

    print("!!!", "Sending to elastic", datetime.datetime.now())
    # Send to elastic
    if not perform_actualize:
        es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", body={
            "settings": DocumentEval.Index.settings,
            "mappings": DocumentEval.Index.mappings
        }
        )

    def doc_eval_generator(documents_criterion_dict):
        for doc in documents_criterion_dict.keys():
            eval = DocumentEval()
            val = documents_criterion_dict[doc]["eval_value"] / documents_criterion_dict[doc]["total_topic_weight"]
            eval.topic_ids_top = [v['topic_id'] for v in documents_criterion_dict[doc]["topic_ids_top"]]
            if criterion.value_range_from < 0:
                eval.topic_ids_bottom = [v['topic_id'] for v in documents_criterion_dict[doc]["topic_ids_bottom"]]
            eval.value = val
            eval.document_es_id = doc
            eval.document_datetime = documents_criterion_dict[doc]["document_datetime"]
            eval.document_source = documents_criterion_dict[doc]["document_source"]
            yield eval.to_dict()

    failed = 0
    success = 0
    total_created = 0
    for ok, result in parallel_bulk(ES_CLIENT, doc_eval_generator(documents_criterion_dict),
                                     index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}",
                                     chunk_size=50000, raise_on_error=True, thread_count=6):
        if not ok:
            failed += 1
        else:
            success += 1
            total_created += 1
        if (failed+success) % 50000 == 0:
            print(f"!!!{failed+success}/{len(documents_criterion_dict.keys())} processed")
        if failed > 5:
            raise Exception("Too many failed ES!!!")
    return total_created
