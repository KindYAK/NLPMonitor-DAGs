"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json
from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.models import Variable

from dags.criterion_eval_init_unique_indices_to_delete.evaluate.service import init_uniques

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

# dag = DAG('Criterion_evaluations', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval='30 14 * * *')
dag = DAG('Criterion_eval_init_unique_indices_to_delete', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

with dag:
    criterions = json.loads(Variable.get('criterions', default_var="[]"))
    evaluators = []
    for criterion in criterions:
        for tm in criterion['topic_modellings']:
            filtered_criterion_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                                criterion['name_translit'].replace(":", "_").replace(" ", "_"))))
            filtered_topic_modelling = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                                          tm['name_translit'].replace(":", "_").replace(" ", "_"))))
            evaluators.append(DjangoOperator(
                task_id=f"eval_{filtered_criterion_name}_{filtered_topic_modelling}",
                python_callable=init_uniques,
                op_kwargs={
                    "criterion_id": criterion['id'],
                    "topic_modelling": tm['name'],
                }
            )
            )
            if criterion['calc_virt_negative']:
                evaluators.append(DjangoOperator(
                    task_id=f"eval_{filtered_criterion_name}_{filtered_topic_modelling}_neg",
                    python_callable=init_uniques,
                    op_kwargs={
                        "criterion_id": criterion['id'],
                        "topic_modelling": tm['name'],
                        "calc_virt_negative": True
                    }
                )
                )
