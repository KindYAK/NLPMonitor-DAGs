"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.astana_test.external_file_example.my_package import test

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'pool': 'short_tasks',
}

dag = DAG('Astana_test', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)


with dag:
    simple_op = PythonOperator(
        task_id="test_simple",
        python_callable=lambda: "Hello, NurSultan!",
        queue='second'
    )

    django_op = DjangoOperator(
        task_id="test_django",
        python_callable=test,
        queue='second'
    )
    simple_op >> django_op
