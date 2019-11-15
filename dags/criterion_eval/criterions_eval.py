"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

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

dag = DAG('Criterion_evaluations', default_args=default_args, schedule_interval='30 14 * * *')


with dag:
    criterions = json.loads(Variable.get('criterions', default_var="[]"))
    evaluators = []
    for criterion in criterions:
        filtered_name = "".join(list(filter(lambda x: x.isalpha() or x in ['.', '-', '_'],
                                            criterion['name_translit'].replace(":", "_").replace(" ", "_"))))
        evaluators.append(DjangoOperator(
            task_id=f"eval_{filtered_name}",
            python_callable=evaluate,
            op_kwargs={
                "criterion_id": criterion['id']
            }
        )
        )
