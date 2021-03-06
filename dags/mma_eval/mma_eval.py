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
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 90,
    'pool': 'short_tasks'
}

dag = DAG('Calc_mma_eval', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

mmas = []
with dag:
    mmas.append(DjangoOperator(
        task_id=f"calculate_mma",
        python_callable=calc_mma,
        op_kwargs={
            "topic_modelling_name": "bigartm_two_years_main_and_gos2",
            "criterion_ids": (1, 35, 34),
            "criterion_weights": ((0.44, 0.33, 0.23), ),
            "class_ids": (36, ),
            "perform_actualize": False
        }
    ))

    mmas.append(DjangoOperator(
        task_id=f"calculate_mma_surveys",
        python_callable=calc_mma,
        op_kwargs={
            "topic_modelling_name": "bigartm_two_years_main_and_gos2",
            "criterion_ids": (1, 35, 34, 37), # Тональность (негатив!!), Резонансность, Гос. программы, Опросы
            "criterion_weights": (
                (0.44, 0.33, 0, 0.23),
                (0, 0.2, 0.4, 0.4),
                (-0.44, 0.33, 0, 0.23),
                (0.36, 0.27, 0.18, 0.18),
                (-0.36, 0.27, 0.18, 0.18),
            ),
            "class_ids": (
                38,
                39,
                40,
                41,
                42,
            ),
            "perform_actualize": False
        }
    ))
