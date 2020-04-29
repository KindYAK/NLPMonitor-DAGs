"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.scraper.scrap.service import scrap


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 50,
    'pool': 'long_tasks',
}

dag = DAG('Scrapers_scrap', catchup=False, max_active_runs=3, default_args=default_args, schedule_interval='15 12 * * *')
dag_full = DAG('Scrapers_scrap_full', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
default_args_fast = default_args.copy()
default_args_fast['pool'] = 'short_tasks'
default_args_fast['priority_weight'] = 60
default_args_fast['retry_delay'] = timedelta(minutes=1)
dag_fast = DAG('Scrapers_scrap_fast', catchup=False, max_active_runs=1, default_args=default_args_fast, schedule_interval='0 * * * *')

sources = json.loads(Variable.get('sources', default_var="[]"))

with dag:
    scrapers = []
    for source in sources:
        filtered_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                            source['name'].replace(":", "_"))))
        scrapers.append(DjangoOperator(
            task_id=f"scrap_{filtered_name}",
            python_callable=scrap,
            op_kwargs={
                "source_url": source['url'],
                "source_id": source['id'],
                "perform_full": False,
                "perform_fast": False,
            }
        )
        )

with dag_full:
    scrapers_full = []
    for source in filter(lambda x: x['perform_full'], sources):
        filtered_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                            source['name'].replace(":", "_"))))
        scrapers_full.append(DjangoOperator(
            task_id=f"scrap_{filtered_name}",
            python_callable=scrap,
            op_kwargs={
                "source_url": source['url'],
                "source_id": source['id'],
                "perform_full": True,
                "perform_fast": False,
            }
        )
        )

with dag_fast:
    scrapers_fast = []
    for source in sources:
        filtered_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                            source['name'].replace(":", "_"))))
        scrapers_full.append(DjangoOperator(
            task_id=f"scrap_{filtered_name}",
            python_callable=scrap,
            op_kwargs={
                "source_url": source['url'],
                "source_id": source['id'],
                "perform_full": False,
                "perform_fast": True,
            }
        )
        )