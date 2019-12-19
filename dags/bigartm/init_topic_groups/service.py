def transliterate_for_dag_id(name):
    from transliterate import translit
    name_translit = translit(name, 'ru', reversed=True)
    return clear_symbols(name_translit)


def clear_symbols(name):
    return "".join([c for c in name.replace(" ", "_") if (c.isalnum() or c in ["_", ".", "-"]) and c not in "әғқңөұүі"]).strip().lower()


def init_topic_groups(**kwargs):
    from airflow.models import Variable
    from mainapp.models_user import TopicGroup
    import json

    groups = TopicGroup.objects.all()
    Variable.set("topic_groups",
                 json.dumps(
                     [{
                         "id": group.id,
                         "name": group.name,
                         "name_translit": transliterate_for_dag_id(group.name),
                         "topic_modelling_name": clear_symbols(group.topic_modelling_name),
                         "topics": [topic.topic_id for topic in group.topics.all()],
                         "owner": group.owner.username,
                         "is_public": group.is_public,
                     } for group in groups]
                 )
                 )
