from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.mma_eval.services.calc_mma import calc_mma

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

dag = DAG('Calc_mma_eval', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

with dag:

    evaluator = DjangoOperator(
        task_id=f"calculate_mma",
        python_callable=calc_mma,
        op_kwargs={
            "topic_modellings_list": ("bigartm_two_years", ),
            "criterion_ids_list": ((1, ), ),
            "perform_actualize": False
        }
    )
