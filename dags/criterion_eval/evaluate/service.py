def evaluate(**kwargs):
    from evaluation.models import EvalCriterion, TopicsEval, TopicIDEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_DOCUMENT

    from elasticsearch_dsl import Search

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

    # Eval documents
    # Dict Document -> [topic_weight*topic_eval for ...]
    documents_criterion_dict = {}
    for tm in criterions_evals_dict.keys():
        std = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_DOCUMENT)
        std = std.filter("term", **{"topic_modelling.keyword": tm}) \
                  .filter("range", topic_weight={"gte": 0.001}) \
                  .source(['document_es_id', 'topic_weight', 'topic_id']).scan()
        for td in std:
            if td.topic_id not in criterions_evals_dict[tm]:
                continue
            if td.document_es_id not in documents_criterion_dict:
                documents_criterion_dict[td.document_es_id] = []
            documents_criterion_dict[td.document_es_id].append(td.topic_weight*criterions_evals_dict[tm][td.topic_id])

    for doc in documents_criterion_dict:
        documents_criterion_dict[doc] = sum(documents_criterion_dict[doc]) / len(documents_criterion_dict[doc])

    print("!!!", documents_criterion_dict)

    return "Done"
