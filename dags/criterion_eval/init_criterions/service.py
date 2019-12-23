def init_criterions(**kwargs):
    from airflow.models import Variable
    from evaluation.models import EvalCriterion
    from mainapp.models_user import TopicID
    import json
    from transliterate import translit

    criterions = EvalCriterion.objects.all()
    Variable.set("criterions",
                     json.dumps(
                         [{
                             "id": criterion.id,
                             "name": criterion.name,
                             "name_translit": translit(criterion.name, 'ru', reversed=True),
                             "value_range_from": criterion.value_range_from,
                             "value_range_to": criterion.value_range_to,
                             "is_integer": criterion.is_integer,
                             "is_categorical": criterion.is_categorical,
                             "topic_modellings": list(set([topic_id.topic_modelling_name for topic_id in TopicID.objects.filter(topicseval__criterion=criterion).order_by("id")])),
                             "topic_modellings_translit": list(set([translit(topic_id.topic_modelling_name, 'ru', reversed=True)
                                                                    for topic_id in TopicID.objects.filter(topicseval__criterion=criterion).order_by("id")])),
                         } for criterion in criterions]
                     )
                 )
