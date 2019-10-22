"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.add_topics_info.service.service import add_topics_info


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 80,
    'pool': 'short_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('TM_Add_Topic_Info', default_args=default_args, schedule_interval='23 */2 * * *')


with dag:
    add_topics_info = DjangoOperator(
        task_id="add_topics_info",
        python_callable=add_topics_info,
    )
