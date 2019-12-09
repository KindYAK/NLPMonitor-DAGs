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
    'pool': 'short_tasks',
}

dag = DAG('Scrapers_scrap', catchup=False, concurrency=1, default_args=default_args, schedule_interval='0 19 * * *')


with dag:
    sources = json.loads(Variable.get('sources', default_var="[]"))
    scrapers = []
    for source in sources:
        filtered_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                            source['name'].replace(":", "_"))))
        scrapers.append(DjangoOperator(
            task_id=f"scrap_{filtered_name}",
            python_callable=scrap,
            op_kwargs={
                "source_url": source['url'],
                "source_id": source['id']
            }
        )
        )
