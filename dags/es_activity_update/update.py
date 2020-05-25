"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.es_activity_update.update.service import es_update


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

dag = DAG('NLPMonitor_es_activity_update', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='15 6 * * *')

indices = json.loads(Variable.get('indices_update_activity', default_var="[]"))

with dag:
    updaters = []
    for index in indices:
        updaters.append(DjangoOperator(
            task_id=f"update_{index}",
            python_callable=es_update,
            op_kwargs={
                "index": index,
            }
        )
        )
