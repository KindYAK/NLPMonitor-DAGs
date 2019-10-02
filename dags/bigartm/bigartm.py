"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 50,
    'pool': 'long_tasks'
}


def gen_bigartm_dag(dag, name, corpus, source, number_of_topics):
    from dags.bigartm.bigartm.service import dataset_prepare, topic_modelling

    # if source:
    #     source = source.split("/")[2]
    with dag:
        dataset_prepare = DjangoOperator(
            task_id="dataset_prepare",
            python_callable=dataset_prepare,
            op_kwargs={
                "name": name,
                "corpus": corpus,
                "source": source,
                "is_ready": False,
                "description": "Simple BigARTM TM",
                "datetime_created": datetime.now(),
                "algorithm": "BigARTM",
                "meta_parameters": {
                },
                "hierarchical": False,
                "number_of_topics": number_of_topics,
            }
        )
        topic_modelling = DjangoOperator(
            task_id="topic_modelling",
            python_callable=topic_modelling,
            op_kwargs={
                "name": name,
                "corpus": corpus,
                "source": source,
            }
        )
        dataset_prepare >> topic_modelling


dag1 = DAG('NLPmonitor_BigARTM', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag1, name="bigartm_test", corpus="main", source=None, number_of_topics=250)

dag2 = DAG('NLPmonitor_BigARTM_tengrinews', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag2, name="bigartm_tengrinews", corpus="main", source="https://tengrinews.kz/", number_of_topics=250)
