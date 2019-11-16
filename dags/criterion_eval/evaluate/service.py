def evaluate(**kwargs):
    import datetime
    import logging

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT
    from mainapp.documents import DocumentEval

    from elasticsearch_dsl import Search
    from elasticsearch.helpers import parallel_bulk

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    print("!!!", "Forming topic-eval dict", datetime.datetime.now())
    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])
    evaluations = TopicsEval.objects.filter(criterion=criterion).prefetch_related('topics')

    # Topic_modelling -> Topic -> [List of evaluations by each author]
    criterions_evals_dict = {}
    for evaluation in evaluations:
        if not evaluation.topics.exists():
            continue
        eval_tm = evaluation.topics.first().topic_modelling_name
        eval_topic_id = evaluation.topics.first().topic_id
        if eval_tm not in criterions_evals_dict:
            criterions_evals_dict[eval_tm] = {}
        if eval_topic_id not in criterions_evals_dict[eval_tm]:
            criterions_evals_dict[eval_tm][eval_topic_id] = []
        criterions_evals_dict[eval_tm][eval_topic_id].append(evaluation.value)

    for tm in criterions_evals_dict.keys():
        for t in criterions_evals_dict[tm].keys():
            criterions_evals_dict[tm][t] = sum(criterions_evals_dict[tm][t]) / len(criterions_evals_dict[tm][t])

    total_created = 0
    for tm in criterions_evals_dict.keys():
        print("!!!", "Forming doc-eval dict for tm", tm, datetime.datetime.now())
        # Eval documents
        # Dict Document -> [topic_weight*topic_eval for ...]
        documents_criterion_dict = {}

        std = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_DOCUMENT)
        std = std.filter("term", **{"topic_modelling.keyword": tm}) \
                  .filter("range", topic_weight={"gte": 0.001}) \
                  .source(['document_es_id', 'topic_weight', 'topic_id', "datetime", "document_source"]).scan()
        for td in std:
            if td.topic_id not in criterions_evals_dict[tm]:
                continue
            if td.document_es_id not in documents_criterion_dict:
                documents_criterion_dict[td.document_es_id] = {
                    "value": [],
                    "document_datetime": td.datetime if hasattr(td, "datetime") else None,
                    "document_source": td.document_source
                }
            documents_criterion_dict[td.document_es_id]["value"].append(
                td.topic_weight * criterions_evals_dict[tm][td.topic_id]
            )

        print("!!!", "Sending to elastic for tm", tm, datetime.datetime.now())
        # Send to elastic
        try:
            Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT_EVAL).filter("term", criterion_id=criterion.id)\
                .filter("term", topic_modelling=tm).delete()
        except:
            print("!!!!!", "Problem during old topic_documents deletion occurred")

        def doc_eval_generator(documents_criterion_dict, tm):
            for doc in documents_criterion_dict.keys():
                eval = DocumentEval()
                val = (sum(documents_criterion_dict[doc]["value"]) / len(documents_criterion_dict[doc]["value"]))
                eval.topic_modelling = tm
                eval.criterion_id = criterion.id
                eval.criterion_name = criterion.name
                eval.value = val
                eval.document_es_id = doc
                eval.document_datetime = documents_criterion_dict[doc]["document_datetime"]
                eval.document_source = documents_criterion_dict[doc]["document_source"]
                yield eval.to_dict()

        failed = 0
        ok = 0
        for ok, result in parallel_bulk(ES_CLIENT, doc_eval_generator(documents_criterion_dict, tm),
                                         index=ES_INDEX_DOCUMENT_EVAL,
                                         chunk_size=50000, raise_on_error=True, thread_count=6):
            if not ok:
                failed += 1
            else:
                ok += 1
                total_created += 1
            if (failed+ok) % 50000 == 0:
                print(f"!!!{failed+ok}/{len(documents_criterion_dict.keys())} processed")
            if failed > 5:
                raise Exception("Too many failed ES!!!")
    return total_created
