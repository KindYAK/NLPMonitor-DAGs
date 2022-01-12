"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.models import Variable

from dags.criterion_eval.evaluate.service import evaluate

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 14),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 95,
    'pool': 'long_tasks',
}

dag = DAG('Criterion_evaluations', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

actualizable_criterion_evals = []
with dag:
    criterions = json.loads(Variable.get('criterions', default_var="[]"))
    evaluators = []
    for criterion in criterions:
        for tm in criterion['topic_modellings']:
            filtered_criterion_name = "".join(list(filter(lambda x: x.isalnum() or x in ['.', '-', '_'],
                                                criterion['name_translit'].replace(":", "_").replace(" ", "_"))))
            filtered_topic_modelling = "".join(list(filter(lambda x: x.isalnum() or x in ['.', '-', '_'],
                                                          tm['name_translit'].replace(":", "_").replace(" ", "_"))))
            evaluators.append(DjangoOperator(
                task_id=f"eval_{filtered_criterion_name}_{filtered_topic_modelling}",
                python_callable=evaluate,
                op_kwargs={
                    "criterion_id": criterion['id'],
                    "topic_modelling": tm['name'],
                }
            )
            )
            # actualizable_criterion_evals.append(
            #     {
            #         "criterion_id": criterion['id'],
            #         "criterion_name": filtered_criterion_name,
            #         "topic_modelling": tm['name'],
            #         "topic_modelling_translit": filtered_topic_modelling,
            #     }
            # )
            if criterion['calc_virt_negative']:
                evaluators.append(DjangoOperator(
                    task_id=f"eval_{filtered_criterion_name}_{filtered_topic_modelling}_neg",
                    python_callable=evaluate,
                    op_kwargs={
                        "criterion_id": criterion['id'],
                        "topic_modelling": tm['name'],
                        "calc_virt_negative": True
                    }
                )
                )
                # actualizable_criterion_evals.append(
                #     {
                #         "criterion_id": criterion['id'],
                #         "criterion_name": filtered_criterion_name,
                #         "topic_modelling": tm['name'],
                #         "topic_modelling_translit": filtered_topic_modelling,
                #         "calc_virt_negative": True
                #     }
                # )
