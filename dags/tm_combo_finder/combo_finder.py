"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.bigartm.bigartm import comboable_bigartms
from dags.tm_combo_finder.services.combo_finder import find_combos

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 14),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 40,
    'pool': 'short_tasks'
}

dag = DAG('NLPmonitor_TM_Combo_Finder', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

combo_finders = []
with dag:
    for tm in comboable_bigartms:
        bigartm_calc_operator = DjangoOperator(
            task_id=f"tm_combo_{tm['name'] if not tm['name_translit'] else tm['name_translit']}",
            python_callable=find_combos,
            op_kwargs={
                "name": tm['name'],
                "name_translit": tm['name_translit'],
            }
        )
        combo_finders.append(bigartm_calc_operator)
