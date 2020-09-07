"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.examples.external_file_example.postgres_io import postgres_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'pool': 'short_tasks',
}

dag = DAG('Example_postgres_io', default_args=default_args, schedule_interval=None)


with dag:
    django_op = DjangoOperator(
        task_id="Postgres_ETL",
        python_callable=postgres_etl,
        op_kwargs={"stuff": "stuff))"}
    )
