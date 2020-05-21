from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.eval_dicts.services.calc_dict import calc_eval_dicts

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

dag = DAG('Generate_eval_dicts', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

with dag:

    evaluator = DjangoOperator(
        task_id=f"calc_eval_dicts",
        python_callable=calc_eval_dicts,
        op_kwargs={
            "topic_modellings_list": ("bigartm_two_years", ),
        }
    )
