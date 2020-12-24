"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.pre_caching.services.service import pre_cache


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 80,
    'pool': 'short_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Nlpmonitor_pre_cache', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 * * * *')


with dag:
    send_elastic = DjangoOperator(
        task_id="pre_cache_dashboard",
        python_callable=pre_cache,
    )
