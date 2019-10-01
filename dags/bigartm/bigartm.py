"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.bigartm.bigartm.service import dataset_prepare, topic_modelling


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 50,
    'pool': 'long_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('NLPmonitor_BigARTM', default_args=default_args, schedule_interval=None)

with dag:
    name = "bigartm_test"
    dataset_prepare = DjangoOperator(
        task_id="dataset_prepare",
        python_callable=dataset_prepare,
        op_kwargs={
            "name": name,
            "corpus": "main",
            "is_ready": False,
            "description": "Simple BigARTM TM",
            "datetime_created": datetime.now(),
            "algorithm": "BigARTM",
            "meta_parameters": {

            },
            "hierarchical": False,
            "number_of_topics": 250
        }
    )
    topic_modelling = DjangoOperator(
        task_id="topic_modelling",
        python_callable=topic_modelling,
        op_kwargs={
            "name": name,
            "corpus": "main"
        }
    )
    dataset_prepare >> topic_modelling
