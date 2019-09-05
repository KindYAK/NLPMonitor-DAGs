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
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 50
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Scrapers_scrap', default_args=default_args, schedule_interval='0 1 * * *')


with dag:
    sources = json.loads(Variable.get('sources'))
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
