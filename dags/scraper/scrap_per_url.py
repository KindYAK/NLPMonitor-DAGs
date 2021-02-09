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
    # 'queue': 'second',
}

dag = DAG('Scrapers_scrap_per_url', catchup=False, max_active_runs=2, default_args=default_args, schedule_interval=None)

# os.path.join(BASE_DAG_DIR, "tmp", f"urls_{source_id}.txt")
with dag:
    scraper1 = DjangoOperator(
        task_id=f"scrap_svoboda",
        python_callable=scrap,
        op_kwargs={
            "source_id": 67,
        }
    )

    scraper2 = DjangoOperator(
        task_id=f"scrap_rt",
        python_callable=scrap,
        op_kwargs={
            "source_id": 60,
        }
    )

    scraper3 = DjangoOperator(
        task_id=f"scrap_sputnik",
        python_callable=scrap,
        op_kwargs={
            "source_id": 71,
        }
    )
