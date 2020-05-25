def init_indices(**kwargs):
    import json

    from airflow.models import Variable

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_DOCUMENT

    indices = ES_CLIENT.indices.get_alias(f"{ES_INDEX_TOPIC_DOCUMENT}_*").keys()
    Variable.set("indices_update_activity",
                     json.dumps(
                         [index for index in indices]
                     )
                 )
