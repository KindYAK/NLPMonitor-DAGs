"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.es_activity_update.init_indices.service import init_indices


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 95,
    'pool': 'short_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('NLPMonitor_es_activity_update_init_indices', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 6 * * *')


with dag:
    init_sources = DjangoOperator(
        task_id="init_indices",
        python_callable=init_indices
    )
