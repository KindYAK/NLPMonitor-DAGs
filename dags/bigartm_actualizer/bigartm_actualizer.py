"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.bigartm import actualizable_bigartms
from dags.bigartm.services.service import bigartm_calc

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 90,
    'pool': 'short_tasks'
}

dag = DAG('NLPmonitor_Actualize_BigARTM', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='30 22 * * *')
dag_fast = DAG('NLPmonitor_Actualize_BigARTM_fast', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='35 * * * *')

actualizers_calcs = []
with dag:
    wait_for_basic_tms = PythonOperator(
        task_id="wait_for_basic_tms",
        python_callable=lambda: 0,
    )

    for tm in actualizable_bigartms:
        bigartm_calc_operator = DjangoOperator(
            task_id=f"bigartm_actualize_{tm['name'] if not tm['name_translit'] else tm['name_translit']}",
            python_callable=bigartm_calc,
            op_kwargs={
                "perform_actualize": True,
                "name": tm['name'],
                "name_translit": tm['name_translit'],
                "text_field": tm['text_field'],
                "corpus": tm["filters"]['corpus'],
                "datetime_from": tm["filters"]['datetime_from'],
                "datetime_to": tm["filters"]['datetime_to'],
                "source": tm["filters"]['source'],
                "group_id": tm["filters"]['group_id'] if 'group_id' in tm["filters"] else None,
                "topic_weight_threshold": tm["filters"]['topic_weight_threshold'] if 'topic_weight_threshold' in tm["filters"] else 0.05,
                "regularization_params": tm["regularization_params"],
            }
        )
        actualizers_calcs.append(bigartm_calc_operator)
        if 'group_id' in tm['filters'] and tm["filters"]['group_id']:
            wait_for_basic_tms >> bigartm_calc_operator
        else:
            bigartm_calc_operator >> wait_for_basic_tms

fast_tms = [
    "bigartm_2020",
    "bigartm_education_2019",
    "bigartm_education_2_2020",
    "bigartm_education_3_2020",
]
actualizers_calcs_fast = []
with dag_fast:
    wait_for_basic_tms = PythonOperator(
        task_id="wait_for_basic_tms",
        python_callable=lambda: 0,
    )

    for tm in filter(lambda x: x['name'] in fast_tms, actualizable_bigartms):
        bigartm_calc_operator = DjangoOperator(
            task_id=f"bigartm_actualize_{tm['name'] if not tm['name_translit'] else tm['name_translit']}",
            python_callable=bigartm_calc,
            op_kwargs={
                "perform_actualize": True,
                "name": tm['name'],
                "name_translit": tm['name_translit'],
                "text_field": tm['text_field'],
                "corpus": tm["filters"]['corpus'],
                "datetime_from": tm["filters"]['datetime_from'],
                "datetime_to": tm["filters"]['datetime_to'],
                "source": tm["filters"]['source'],
                "group_id": tm["filters"]['group_id'] if 'group_id' in tm["filters"] else None,
                "topic_weight_threshold": tm["filters"]['topic_weight_threshold'] if 'topic_weight_threshold' in tm["filters"] else 0.05,
                "regularization_params": tm["regularization_params"],
                "fast": True,
            }
        )
        actualizers_calcs.append(bigartm_calc_operator)
        if 'group_id' in tm['filters'] and tm["filters"]['group_id']:
            wait_for_basic_tms >> bigartm_calc_operator
        else:
            bigartm_calc_operator >> wait_for_basic_tms
