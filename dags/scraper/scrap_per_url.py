"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.scraper.scrap.service_per_url import scrap


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 40,
    'pool': 'scraper_tasks',
}

dag = DAG('Scrapers_scrap_per_url', catchup=False, max_active_runs=2, default_args=default_args, schedule_interval=None)

with dag:
    scraper = DjangoOperator(
        task_id=f"scrap_dw",
        python_callable=scrap,
        op_kwargs={
            "source_id": 70,
        }
    )
