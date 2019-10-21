"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, date, timedelta

from dags.mussabayev_tm.service.service import generate_coocurance_codistance


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 55,
    'pool': 'long_tasks'
}

dag = DAG('Nlpmonitor_Mussabayev_tm', default_args=default_args, schedule_interval=None)


with dag:
    generate_coocurance_codistance = DjangoOperator(
        task_id="generate_coocurance_codistance",
        python_callable=generate_coocurance_codistance,
        op_kwargs={
            "name": "test",
            "dictionary_filters": {
                "document_normal_frequency__gte": 1,
                "document_normal_frequency__lte": 500000,
                "is_stop_word": False,
                # "is_in_pymorphy2_dict": True,
                # "is_multiple_normals_in_pymorphy2": False,
            },
            "max_dict_size": 30000,
            "document_filters": {
                "corpus": "main",
                # "source": "https://kapital.kz/",
                "datetime__gte": date(1950, 1, 1),
                "datetime__lte": date(2050, 1, 1),
            }
        }
    )
