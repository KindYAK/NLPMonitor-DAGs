"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.criterion_eval.criterions_eval import actualizable_criterion_evals
from dags.criterion_eval.evaluate.service import evaluate

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

dag = DAG('Criterion_actualize_evaluations', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='0 23 * * *')

actualizers_evaluators = []
with dag:
    for eval in actualizable_criterion_evals:
        evaluator = DjangoOperator(
            task_id=f"eval_actualize_{eval['criterion_name']}_{eval['topic_modelling_translit']}",
            python_callable=evaluate,
            op_kwargs={
                "perform_actualize": True,
                "criterion_id": eval["criterion_id"],
                "topic_modelling": eval["topic_modelling"],
            }
        )
        actualizers_evaluators.append(evaluator)
        if 'calc_virt_negative' in eval:
            evaluator = DjangoOperator(
                task_id=f"eval_actualize_{eval['criterion_name']}_{eval['topic_modelling_translit']}_neg",
                python_callable=evaluate,
                op_kwargs={
                    "perform_actualize": True,
                    "criterion_id": eval["criterion_id"],
                    "topic_modelling": eval["topic_modelling"],
                    "calc_virt_negative": True,
                }
            )
