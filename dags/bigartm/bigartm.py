"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta, date


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


def gen_bigartm_dag(dag, name, description, number_of_topics, filters, regularization_params):
    from dags.bigartm.bigartm.service import dataset_prepare, topic_modelling

    # if source:
    #     source = source.split("/")[2]
    with dag:
        dataset_prepare = DjangoOperator(
            task_id="dataset_prepare",
            python_callable=dataset_prepare,
            op_kwargs={
                "name": name,
                "corpus": filters['corpus'],
                "source": filters['source'],
                "datetime_from": filters['datetime_from'],
                "datetime_to": filters['datetime_to'],
                "is_ready": False,
                "description": description,
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
                "corpus": filters['corpus'],
                "source": filters['source'],
                "datetime_from": filters['datetime_from'],
                "datetime_to": filters['datetime_to'],
                "regularization_params": regularization_params,
            }
        )
        dataset_prepare >> topic_modelling


dag1 = DAG('NLPmonitor_BigARTM', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag1, name="bigartm_test", description="All news", number_of_topics=250,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": None,
                    "datetime_to": None,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                })

dag2 = DAG('NLPmonitor_BigARTM_tengrinews', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag2, name="bigartm_tengrinews", description="All news from tengrinews", number_of_topics=250,
                filters={
                    "corpus": "main",
                    "source": "https://tengrinews.kz/",
                    "datetime_from": None,
                    "datetime_to": None,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                })

dag3 = DAG('NLPmonitor_BigARTM_small_test', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag3, name="bigartm_small_test", description="Subset of tengrinews news", number_of_topics=250,
                filters={
                    "corpus": "main",
                    "source": "https://kapital.kz/",
                    "datetime_from": date(2019, 1, 1),
                    "datetime_to": date(2019, 3, 1),
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                })

dag4 = DAG('NLPmonitor_BigARTM_less_small_test', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag4, name="bigartm_less_small_test", description="Subset of tengrinews news", number_of_topics=250,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2019, 1, 1),
                    "datetime_to": date(2019, 3, 1),
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                })

dag4 = DAG('NLPmonitor_BigARTM_two_years', default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag4, name="bigartm_two_years", description="Two last years", number_of_topics=200,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2017, 11, 1),
                    "datetime_to": date(2019, 12, 1),
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                })
