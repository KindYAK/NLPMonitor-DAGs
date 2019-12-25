"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.bigartm import actualizable_bigartms
from dags.bigartm.services.service import calc_topics_info

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 90,
    'pool': 'short_tasks'
}

dag = DAG('NLPmonitor_get_topics_info', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

topic_info_getters = []
with dag:
    for tm in actualizable_bigartms:
        topic_info_getter = DjangoOperator(
            task_id=f"get_topics_info_{tm['name']}",
            python_callable=calc_topics_info,
            op_kwargs={
                "corpus": tm["filters"]['corpus'],
                "topic_modelling_name": tm['name'],
                "topic_weight_threshold": tm["filters"]['topic_weight_threshold'] if 'topic_weight_threshold' in tm["filters"] else None,
            }
        )
        topic_info_getters.append(topic_info_getter)
