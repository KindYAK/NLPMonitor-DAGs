from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 50,
    'pool': 'long_tasks'
}


def gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name, description, number_of_topics, filters, regularization_params, wait_for_basic_tms,
                         is_actualizable=False, name_translit=None, topic_modelling_translit=None, is_comboable=True,
                         text_field="text_lemmatized"):
    from dags.bigartm.services.service import bigartm_calc

    if not name_translit:
        task_id = f"bigartm_calc_{name}"
    else:
        task_id = f"bigartm_calc_{topic_modelling_translit}_{name_translit}"
    bigartm_calc_operator = DjangoOperator(
        task_id=task_id,
        python_callable=bigartm_calc,
        op_kwargs={
            "name": name,
            "name_translit": name_translit,
            "text_field": text_field,
            "corpus": filters['corpus'],
            "corpus_datetime_ignore": filters.get('corpus_datetime_ignore', []),
            "source": filters['source'],
            "datetime_from": filters['datetime_from'],
            "datetime_to": filters['datetime_to'],
            "group_id": filters['group_id'] if 'group_id' in filters else None,
            "topic_weight_threshold": filters['topic_weight_threshold'] if 'topic_weight_threshold' in filters else 0.05,
            "is_ready": False,
            "description": description,
            "datetime_created": datetime.now(),
            "algorithm": "BigARTM",
            "meta_parameters": {

            },
            "number_of_topics": number_of_topics,
            "regularization_params": regularization_params,
            "is_actualizable": is_actualizable,
        }
    )
    if 'group_id' in filters and filters['group_id']:
        wait_for_basic_tms >> bigartm_calc_operator
    else:
        bigartm_calc_operator >> wait_for_basic_tms
    if is_actualizable:
        actualizable_bigartms.append(
            {
                "name": name,
                "name_translit": name_translit,
                "text_field": text_field,
                "regularization_params": regularization_params.copy(),
                "filters": filters.copy()
            }
        )
    if is_comboable:
        comboable_bigartms.append(
            {
                "name": name,
                "name_translit": name_translit,
                "text_field": text_field,
            }
        )
