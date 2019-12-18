def init_topic_groups(**kwargs):
    from airflow.models import Variable
    from mainapp.models_user import TopicGroup
    import json
    from transliterate import translit

    groups = TopicGroup.objects.all()
    Variable.set("topic_groups",
                 json.dumps(
                     [{
                         "id": group.id,
                         "name": group.name,
                         "name_translit": translit(group.name, 'ru', reversed=True).replace(" ", "_").strip(),
                         "topic_modelling_name": group.topic_modelling_name,
                         "topics": [topic.topic_id for topic in group.topics.all()],
                         "owner": group.owner.username,
                         "is_public": group.is_public,
                     } for group in groups]
                 )
                 )
