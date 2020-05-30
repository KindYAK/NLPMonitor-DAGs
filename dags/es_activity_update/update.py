"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.es_activity_update.update.service import es_update, init_update_datetime, set_update_datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 40,
    'pool': 'long_tasks',
}

dag = DAG('NLPMonitor_es_activity_update', catchup=False, max_active_runs=1, concurrency=14, default_args=default_args, schedule_interval='15 6 * * 5')

indices = json.loads(Variable.get('indices_update_activity', default_var="[]"))

with dag:
    init_update_datetime = DjangoOperator(
            task_id=f"init_update_datetime",
            python_callable=init_update_datetime,
        )
    updaters = []
    for index in indices:
        updaters.append(DjangoOperator(
            task_id=f"update_{index['name_translit']}",
            python_callable=es_update,
            op_kwargs={
                "index": index['name'],
            }
        )
        )
    set_update_datetime = DjangoOperator(
            task_id=f"set_update_datetime",
            python_callable=set_update_datetime,
        )
    init_update_datetime >> updaters >> set_update_datetime
