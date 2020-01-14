def evaluate(**kwargs):
    import datetime

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, \
        ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS, ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.documents import DocumentEval, DocumentEvalUniqueIDs

    from elasticsearch_dsl import Search, Index
    from elasticsearch.helpers import parallel_bulk

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    print("!!!", "Forming topic-eval dict", datetime.datetime.now())
    topic_modelling = kwargs['topic_modelling']
    perform_actualize = 'perform_actualize' in kwargs
    calc_virt_negative = 'calc_virt_negative' in kwargs
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

    # Eval documents
    # Dict Document -> [topic_weight*topic_eval for ...]
    print("!!!", "Finding IDs to process", datetime.datetime.now())
    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT_UNIQUE_IDS}_{topic_modelling}") \
            .source(['document_es_id'])[:5000000]
    ids_to_process = set((doc.document_es_id for doc in std.scan()))

    ids_to_skip = set()
    if perform_actualize:
        print("!!!", "Performing actualizing, skipping document already in TM", datetime.datetime.now())
        s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}") \
                .source(['document_es_id'])[:5000000]
        ids_to_skip = set((doc.document_es_id for doc in s.scan()))
    ids_to_process = ids_to_process - ids_to_skip

    def doc_eval_generator(ids_to_process):
        current_doc = None
        if criterion.value_range_from < 0:
            range_center = (criterion.value_range_from + criterion.value_range_to) / 2
            neutral_neighborhood = 0.1
        else:
            range_center = 0
            neutral_neighborhood = 0.001
        for document_es_id in ids_to_process:
            s = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}") \
                .filter("range", topic_weight={"gte": 0.001}).filter("term", document_es_id=document_es_id) \
                .source(['topic_weight', 'topic_id', "datetime", "document_source"])
            for td in s.execute():
                if td.topic_id not in criterions_evals_dict:
                    criterion_value = 0
                else:
                    criterion_value = criterions_evals_dict[td.topic_id]
                if current_doc is None:
                    current_doc = {
                        "document_es_id": document_es_id,
                        "document_datetime": td.datetime if hasattr(td, "datetime") and td.datetime else None,
                        "document_source": td.document_source,
                        "eval_value": 0,
                        "total_topic_weight": 0,
                        "topic_ids_top": [],
                        "topic_ids_bottom": [],
                    }
                if not calc_virt_negative:
                    eval = td.topic_weight * criterion_value
                else:
                    if criterion.value_range_from < 0:
                        eval = (-1 * td.topic_weight) * criterion_value
                    else:
                        eval = (1 - td.topic_weight) * criterion_value
                current_doc["eval_value"] += eval
                current_doc["total_topic_weight"] += td.topic_weight
                # Top docs
                if (any((eval > t['eval'] for t in current_doc['topic_ids_top'])) or len(current_doc['topic_ids_top']) < 3) and eval > range_center + neutral_neighborhood:
                    current_doc['topic_ids_top'].append(
                        {
                            "topic_id": td.topic_id,
                            "eval": eval
                        }
                    )
                if len(current_doc['topic_ids_top']) > 3:
                    del current_doc['topic_ids_top'][current_doc['topic_ids_top'].index(
                            min(current_doc['topic_ids_top'], key=lambda x: x['eval'])
                        )]
                # Bottom docs
                if criterion.value_range_from < 0:
                    if any((eval < t['eval'] for t in current_doc['topic_ids_bottom'])) or len(current_doc['topic_ids_bottom']) < 3 and eval < range_center - neutral_neighborhood:
                        current_doc['topic_ids_bottom'].append(
                            {
                                "topic_id": td.topic_id,
                                "eval": eval
                            }
                        )
                    if len(current_doc['topic_ids_bottom']) > 3:
                        del current_doc['topic_ids_bottom'][
                            current_doc['topic_ids_bottom'].index(
                                max(current_doc['topic_ids_bottom'],
                                    key=lambda x: x['eval'])
                            )
                        ]
            yield current_doc
            current_doc = None

    print("!!!", "Sending to elastic + calculating through generators", datetime.datetime.now())
    # Send to elastic
    if not perform_actualize:
        es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}", body={
                "settings": DocumentEval.Index.settings,
                "mappings": DocumentEval.Index.mappings
            }
        )

    def eval_calc_generator(ids_to_process):
        for doc in doc_eval_generator(ids_to_process):
            eval = DocumentEval()
            eval.value = doc["eval_value"] / doc["total_topic_weight"]
            eval.topic_ids_top = [v['topic_id'] for v in doc["topic_ids_top"]]
            if criterion.value_range_from < 0:
                eval.topic_ids_bottom = [v['topic_id'] for v in doc["topic_ids_bottom"]]
            eval.document_es_id = doc['document_es_id']
            eval.document_datetime = doc["document_datetime"]
            eval.document_source = doc["document_source"]
            yield eval.to_dict()

    total_created = 0
    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, eval_calc_generator(ids_to_process),
                                     index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}",
                                     chunk_size=10000 if not perform_actualize else 500, raise_on_error=True, thread_count=4):
        if (failed+success) % 2500 == 0:
            print(f"!!!{failed+success}/{len(ids_to_process)} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1
            total_created += 1

    if perform_actualize and total_created == 0:
        return f"No documents to actualize"

    # Create or update unique IDs index
    print("!!!", "Writing unique IDs", datetime.datetime.now())
    if not perform_actualize:
        es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}", body={
                "settings": DocumentEvalUniqueIDs.Index.settings,
                "mappings": DocumentEvalUniqueIDs.Index.mappings
            }
        )

    def unique_ids_generator(ids_to_process):
        for doc_id in ids_to_process:
            doc = DocumentEvalUniqueIDs()
            doc.document_es_id = doc_id
            yield doc

    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, (doc.to_dict() for doc in unique_ids_generator(ids_to_process)),
                                    index=f"{ES_INDEX_DOCUMENT_EVAL_UNIQUE_IDS}_{topic_modelling}_{criterion.id}{'_neg' if calc_virt_negative else ''}",
                                    chunk_size=10000, raise_on_error=True, thread_count=4):
        if (failed + success) % 10000 == 0:
            print(f"!!!{failed + success}/{len(ids_to_process)} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1
            total_created += 1
    return total_created
