"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.tm_preprocessing.services.tm_preproc_services import preprocessing_raw_data, init_last_datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 66,
    'pool': 'long_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Nlpmonitor_Lemmatization', default_args=default_args, schedule_interval='15 1 * * *')

with dag:
    init_last_datetime = DjangoOperator(
        task_id="init_last_datetime",
        python_callable=init_last_datetime,
    )

    concurrency = 4
    lemmatize_operators = []
    for i in range(concurrency):
        lemmatize_operators.append(DjangoOperator(
            task_id=f"lemmatize_{i}",
            python_callable=preprocessing_raw_data,
            op_kwargs={
                "start": (100 / concurrency) * i,
                "end": (100 / concurrency) * (i + 1)
            }
        ))
    init_last_datetime >> lemmatize_operators
