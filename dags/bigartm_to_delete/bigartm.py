"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json
from datetime import datetime, timedelta, date

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

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
}

actualizable_bigartms = []
bigartm_calc_operators = []
def gen_bigartm_operator(name, description, number_of_topics, filters, regularization_params, is_actualizable=False, name_translit=None, topic_modelling_parent=None):
    from dags.bigartm_to_delete.services.service import bigartm_calc

    if not name_translit:
        task_id = f"bigartm_calc_{name}"
    else:
        task_id = f"bigartm_calc_{topic_modelling_parent}_{name_translit}"
    bigartm_calc_operator = DjangoOperator(
        task_id=task_id,
        python_callable=bigartm_calc,
        op_kwargs={
            "name": name,
            "name_translit": name_translit,
            "corpus": filters['corpus'],
            "source": filters['source'],
            "datetime_from": filters['datetime_from'],
            "datetime_to": filters['datetime_to'],
            "group_id": filters['group_id'] if 'group_id' in filters else None,
            "topic_weight_threshold": filters['topic_weight_threshold'] if 'topic_weight_threshold' in filters else 0.05,
            "is_ready": False,
            "description": description,
            "datetime_created": datetime.now(),
            "algorithm": "BigARTM",
            "meta_parameters": {

            },
            "number_of_topics": number_of_topics,
            "regularization_params": regularization_params,
            "is_actualizable": is_actualizable,
        }
    )

dag = DAG('NLPmonitor_BigARTMs_To_delete', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
with dag:
    gen_bigartm_operator(name="bigartm_two_years", description="Two last years", number_of_topics=200,
                         filters={
                        "corpus": "main",
                        "source": None,
                        "datetime_from": date(2017, 11, 1),
                        "datetime_to": date(2019, 12, 1),
                    },
                         regularization_params={
                        "SmoothSparseThetaRegularizer": 0.15,
                        "SmoothSparsePhiRegularizer": 0.15,
                        "DecorrelatorPhiRegularizer": 0.15,
                        "ImproveCoherencePhiRegularizer": 0.15
                    }, is_actualizable=True)
