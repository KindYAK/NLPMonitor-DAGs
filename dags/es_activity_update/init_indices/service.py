def init_indices(**kwargs):
    import json
    from util.util import transliterate_for_dag_id

    from airflow.models import Variable

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_DOCUMENT

    indices = ES_CLIENT.indices.get_alias(f"{ES_INDEX_TOPIC_DOCUMENT}_*").keys()
    Variable.set("indices_update_activity",
                 json.dumps(
                     [
                         {
                             "name": index,
                             "name_translit": transliterate_for_dag_id(index),
                         }
                         for index in indices
                     ]
                 )
                 )
