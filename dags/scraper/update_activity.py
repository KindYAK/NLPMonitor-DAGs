"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.scraper.scrap.service_update import update


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 18),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 40,
    'pool': 'scraper_tasks',
}

dag = DAG('Scrapers_update_activity', catchup=False, max_active_runs=2, default_args=default_args, schedule_interval='0 6 * * *')

with dag:
    scrapers = []
    concurrency = 4
    for i in range(concurrency):
        scrapers.append(DjangoOperator(
            task_id=f"scrap_{i}",
            python_callable=update,
            op_kwargs={
                "start": (100 / concurrency) * i,
                "end": (100 / concurrency) * (i + 1)
            }
        )
        )
