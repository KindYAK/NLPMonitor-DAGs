def transliterate_for_dag_id(name):
    from transliterate import translit
    name_translit = translit(name, 'ru', reversed=True).replace(" ", "_").strip()
    name_translit = "".join([c for c in name_translit if c.isalnum() or c in ["_", ".", "-"]])
    return name_translit


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
                         "topic_modelling_name": group.topic_modelling_name,
                         "topics": [topic.topic_id for topic in group.topics.all()],
                         "owner": group.owner.username,
                         "is_public": group.is_public,
                     } for group in groups]
                 )
                 )
