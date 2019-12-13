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
                         "topic_modelling_name": group.topic_modelling_name,
                         "topics": [topic.topic_id for topic in group.topics.all()],
                         "owner": group.owner.username,
                         "is_public": group.is_public,
                     } for group in groups]
                 )
                 )
