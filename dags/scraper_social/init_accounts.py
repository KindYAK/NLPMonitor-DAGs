"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.scraper_social.init_accounts.service import init_accounts


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 95,
    'pool': 'scraper_tasks',
    # 'queue': 'second',
}

dag = DAG('Scrapers_init_social_accounts', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 12 * * *')

with dag:
    init_sources = DjangoOperator(
        task_id="init_accounts",
        python_callable=init_accounts,
    )
