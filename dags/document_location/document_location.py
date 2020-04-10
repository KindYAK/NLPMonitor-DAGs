"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.document_location.services.get_locations import get_locations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 90,
    'pool': 'short_tasks'
}

dag = DAG('Generate_document_locations', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 23 * * *')

with dag:

    evaluator = DjangoOperator(
        task_id=f"document_locations",
        python_callable=get_locations,
        op_kwargs={
            "criterion_tm_duos": (("bigartm_two_years", 1), ),

        }
    )
