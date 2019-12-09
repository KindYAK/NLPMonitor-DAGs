"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.bigartm.bigartm import actualizable_bigartms
from dags.bigartm.services.service import dataset_prepare, topic_modelling

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 80,
    'pool': 'short_tasks'
}

dag = DAG('NLPmonitor_Actualize_BigARTM', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

actualizers_prepares = []
actualizers_actualizers = []
with dag:
    for tm in actualizable_bigartms:
        prepare = DjangoOperator(
            task_id=f"prepare_{tm['name']}",
            python_callable=dataset_prepare,
            op_kwargs={
                "perform_actualize": True,
                "name": tm['name'],
                "corpus": tm["filters"]['corpus'],
                "datetime_from": tm["filters"]['datetime_from'],
                "datetime_to": tm["filters"]['datetime_to'],
                "source": tm["filters"]['source'],
                "group_id": tm["filters"]['group_id'] if 'group_id' in tm["filters"] else None,
                "topic_weight_threshold": tm["filters"]['topic_weight_threshold'] if 'topic_weight_threshold' in tm["filters"] else None,
            }
        )
        actualize = DjangoOperator(
            task_id=f"actualize_{tm['name']}",
            python_callable=topic_modelling,
            op_kwargs={
                "perform_actualize": True,
                "name": tm['name'],
                "corpus": tm["filters"]['corpus'],
                "regularization_params": tm["regularization_params"],
            }
        )
        prepare >> actualize
        actualizers_prepares.append(prepare)
        actualizers_actualizers.append(actualize)