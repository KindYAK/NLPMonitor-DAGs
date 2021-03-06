"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.tm_preprocessing_n_gram.services.service import init_last_datetime, ngramize

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 40,
    'pool': 'short_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Nlpmonitor_NGramize', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='15 22 * * *')


def create_tasks(dict_name, source_field, min_document_frequency_relative, max_n_gram_len, concurrency=5):
    init_last_datetime_op = DjangoOperator(
        task_id=f"init_last_datetime_{dict_name}",
        python_callable=init_last_datetime,
        op_kwargs={
            "dict_name": dict_name,
            "source_field": source_field,
        }
    )

    lemmatize_operators = []
    for i in range(concurrency):
        lemmatize_operators.append(DjangoOperator(
            task_id=f"ngramize_{dict_name}_{i}",
            python_callable=ngramize,
            op_kwargs={
                "start": (100 / concurrency) * i,
                "end": (100 / concurrency) * (i + 1),
                "dict_name": dict_name,
                "source_field": source_field,
                "max_n_gram_len": max_n_gram_len,
                "min_document_frequency_relative": min_document_frequency_relative,
            }
        ))
    init_last_datetime_op >> lemmatize_operators


with dag:
    # dict_name = "kz_rus_ngrams_dict_pymorphy_2_4_393442_3710985"
    create_tasks(dict_name="kz_rus_yandex_ngrams_dict",
                 source_field="text_lemmatized_yandex",
                 min_document_frequency_relative=1 / 1000,
                 max_n_gram_len=3)

    create_tasks(dict_name="en_lemminflect",
                 source_field="text_lemmatized_eng_lemminflect",
                 min_document_frequency_relative=1 / 1000,
                 max_n_gram_len=3,
                 concurrency=2)
