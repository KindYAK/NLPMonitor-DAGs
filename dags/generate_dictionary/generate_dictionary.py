"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.generate_dictionary.services.service import generate_dictionary_batch, init_dictionary_index, aggregate_dicts

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=120),
    'priority_weight': 40,
    'pool': 'long_tasks'
}

dag = DAG('Nlpmonitor_Dictionary_Generation', catchup=False, concurrency=6, max_active_runs=1, default_args=default_args, schedule_interval=None)

with dag:
    corpuses = ["scopus_real_real"]
    # name = "kz_rus_ngrams_dict_pymorphy_2_4_393442_3710985"
    # name = "kz_rus_yandex_ngrams_dict"
    # name = "en_lemminflect"
    name = "en_scopus"
    max_n_gram_len = 3
    field_to_parse = "text_lemmatized_eng_lemminflect"

    init_dictionary_index = DjangoOperator(
        task_id="init_dictionary_index",
        python_callable=init_dictionary_index,
        op_kwargs={
            "corpuses": corpuses,
            "name": name,
            "datetime": datetime.now(),
            "max_n_gram_len": max_n_gram_len,
            "field_to_parse": field_to_parse,
        }
    )

    concurrency = 150
    dictionary_operators = []
    for i in range(concurrency):
        dictionary_operators.append(DjangoOperator(
            task_id=f"dictionary_{i}",
            python_callable=generate_dictionary_batch,
            op_kwargs={
                "name": name,
                "process_num": i,
                "total_proc": concurrency,
                "corpuses": corpuses,
                "max_n_gram_len": max_n_gram_len,
                "field_to_parse": field_to_parse,
                "min_relative_document_frequency": 1 / 2_500,
            }
        ))

    aggregate_dicts = DjangoOperator(
            task_id=f"aggragate_dicts",
            python_callable=aggregate_dicts,
            op_kwargs={
                "name": name,
                "corpuses": corpuses,
                "min_relative_document_frequency": 1 / 1_000,
                "total_proc": 1 / concurrency,
            }
        )
    init_dictionary_index >> dictionary_operators >> aggregate_dicts
