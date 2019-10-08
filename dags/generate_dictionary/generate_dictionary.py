"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.generate_dictionary.services.service import generate_dictionary_batch, init_dictionary_index, aggregate_dicts

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 3),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 50,
    'pool': 'long_tasks'
}

dag = DAG('Nlpmonitor_Dictionary_Generation', default_args=default_args, schedule_interval=None)

with dag:
    init_dictionary_index = DjangoOperator(
        task_id="init_dictionary_index",
        python_callable=init_dictionary_index,
        op_kwargs={
            "corpus": "main",
            "name": "default_dict_pymorphy_2_4_393442_3710985",
            "datetime": datetime.now(),
        }
    )

    concurrency = 4
    dictionary_operators = []
    for i in range(concurrency):
        dictionary_operators.append(DjangoOperator(
            task_id=f"dictionary_{i}",
            python_callable=generate_dictionary_batch,
            op_kwargs={
                "name": "default_dict_pymorphy_2_4_393442_3710985",
                "i": i,
                "start": (100 / concurrency) * i,
                "end": (100 / concurrency) * (i + 1)
            }
        ))

    aggregate_dicts = DjangoOperator(
            task_id=f"aggragate_dicts",
            python_callable=aggregate_dicts,
            op_kwargs={
                "name": "default_dict_pymorphy_2_4_393442_3710985",
                "concurrency": concurrency,
            }
        )
    init_dictionary_index >> dictionary_operators >> aggregate_dicts
