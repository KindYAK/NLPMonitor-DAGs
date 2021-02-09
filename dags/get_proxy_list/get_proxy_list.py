"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.get_proxy_list.services.service_proxy import get_proxy_list

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 50,
    'pool': 'scraper_tasks',
    'execution_timeout': timedelta(hours=1),
    # 'queue': 'second',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Scrapers_get_proxy_list', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 12 * * *')

with dag:
    proxy_op = DjangoOperator(
        task_id="get_proxy_list",
        python_callable=get_proxy_list,
        execution_timeout=timedelta(hours=1)
    )
