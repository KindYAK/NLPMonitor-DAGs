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
                             "calc_virt_negative": criterion.calc_virt_negative,
                             "topic_modellings": [
                                 {
                                     "name": topic_id.topic_modelling_name,
                                     "name_translit": translit(topic_id.topic_modelling_name, 'ru', reversed=True)
                                 } for topic_id in TopicID.objects.filter(topicseval__criterion=criterion).distinct()
                             ]
                         } for criterion in criterions]
                     )
                 )
