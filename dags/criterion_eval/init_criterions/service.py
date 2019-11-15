def init_criterions(**kwargs):
    from airflow.models import Variable
    from evaluation.models import EvalCriterion
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
                         } for criterion in criterions]
                     )
                 )
