def evaluate(**kwargs):
    from evaluation.models import EvalCriterion, TopicsEval, TopicIDEval

    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])
    evaluations = TopicsEval.objects.filter(criterion=criterion).prefetch_related('topics')

    # Topic_modelling -> Topic -> [List of evaluations by each author]
    criterions_dict = {}
    for evaluation in evaluations:
        eval_tm = evaluation.topics.first().topic_modelling_name
        eval_topic_id = evaluation.topics.first().topic_id
        if eval_tm not in criterions_dict:
            criterions_dict[eval_tm] = {}
        if eval_topic_id not in criterions_dict[eval_tm]:
            criterions_dict[eval_tm][eval_topic_id] = []
        criterions_dict[eval_tm][eval_topic_id].append(evaluation.value)

    for tm in criterions_dict.keys():
        for t in criterions_dict[tm].keys():
            criterions_dict[tm][t] = sum(criterions_dict[tm][t]) / len(criterions_dict[tm][t])

    return "Done"