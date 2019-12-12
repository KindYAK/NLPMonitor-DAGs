def evaluate(**kwargs):
    import datetime
    import logging

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT, ES_HOST
    from mainapp.documents import DocumentEval

    from elasticsearch_dsl import Search, Index
    from elasticsearch.helpers import parallel_bulk

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    print("!!!", "Forming topic-eval dict", datetime.datetime.now())
    topic_modelling = kwargs['topic_modelling']
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

    total_created = 0
    print("!!!", "Forming doc-eval dict for topic_modelling", topic_modelling, datetime.datetime.now())
    # Eval documents
    # Dict Document -> [topic_weight*topic_eval for ...]
    documents_criterion_dict = {}

    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}")
    std = std.filter("range", topic_weight={"gte": 0.001}) \
              .source(['document_es_id', 'topic_weight', 'topic_id', "datetime", "document_source"]).scan()
    for td in std:
        if td.topic_id not in criterions_evals_dict:
            continue
        if td.document_es_id not in documents_criterion_dict:
            documents_criterion_dict[td.document_es_id] = {
                "value": [],
                "document_datetime": td.datetime if hasattr(td, "datetime") and td.datetime else None,
                "document_source": td.document_source
            }
        documents_criterion_dict[td.document_es_id]["value"].append(
            {
                "topic_weight": td.topic_weight,
                "criterion_value": criterions_evals_dict[td.topic_id],
            }
        )

    print("!!!", "Sending to elastic for topic_modelling", topic_modelling, datetime.datetime.now())
    # Send to elastic
    if "delete_indices" in kwargs and kwargs['delete_indices']:
        es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", body={
            "settings": DocumentEval.Index.settings,
            "mappings": DocumentEval.Index.mappings
        }
        )

    def doc_eval_generator(documents_criterion_dict, topic_modelling):
        for doc in documents_criterion_dict.keys():
            eval = DocumentEval()
            val = (sum([v['topic_weight']*v['criterion_value'] for v in documents_criterion_dict[doc]["value"]])
                    / sum([v['topic_weight'] for v in documents_criterion_dict[doc]["value"]]))
            eval.criterion_name = criterion.name
            eval.value = val
            eval.document_es_id = doc
            eval.document_datetime = documents_criterion_dict[doc]["document_datetime"]
            eval.document_source = documents_criterion_dict[doc]["document_source"]
            yield eval.to_dict()

    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, doc_eval_generator(documents_criterion_dict, topic_modelling),
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
