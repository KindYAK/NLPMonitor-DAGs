"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.cluster_sources.services.service import run_cluster

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 40,
    'pool': 'short_tasks'
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('ML_cluster_sources', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None) # '15 22 * * *'

with dag:
    cluster_operators = []
    cluster_operators.append(DjangoOperator(
        task_id=f"cluster_rus",
        python_callable=run_cluster,
        op_kwargs={
            "tm_name": "bigartm_two_years_rus_and_rus_propaganda",
            "eps_range": [i/10 for i in range(1, 11)],
            "min_samples_range": range(1, 10),
        }
    ))

    cluster_operators.append(DjangoOperator(
        task_id=f"cluster_rus_kz",
        python_callable=run_cluster,
        op_kwargs={
            "tm_name": "bigartm_two_years_rus_and_main",
            "eps_range": [i/10 for i in range(1, 11)],
            "min_samples_range": range(1, 10),
        }
    ))

    cluster_operators.append(DjangoOperator(
        task_id=f"cluster_kz",
        python_callable=run_cluster,
        op_kwargs={
            "tm_name": "bigartm_two_years",
            "eps_range": [i / 10 for i in range(1, 11)],
            "min_samples_range": range(1, 10),
        }
    ))
