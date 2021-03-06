"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from datetime import datetime, timedelta
from dags.examples.external_file_example.my_package import some_complicated_stuff

from PythonVirtualenvCachedOperator import PythonVirtualenvCachedOperator


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

dag = DAG('Example_external_cached_venv_example', default_args=default_args, schedule_interval=None)

with dag:
    test_env_op = PythonVirtualenvCachedOperator(
        task_id="op1",
        python_callable=some_complicated_stuff,
        python_version="3.6",
        requirements=[
            "xlrd==1.2.0",
        ]
    )
