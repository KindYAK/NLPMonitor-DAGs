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

actualizable_bigartms = []
def gen_bigartm_dag(dag, name, description, number_of_topics, filters, regularization_params, is_actualizable=False):
    from dags.bigartm.services.service import dataset_prepare, topic_modelling

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
                "group_id": filters['group_id'] if 'group_id' in filters else None,
                "topic_weight_threshold": filters['topic_weight_threshold'] if 'topic_weight_threshold' in filters else None,
                "is_ready": False,
                "description": description,
                "datetime_created": datetime.now(),
                "algorithm": "BigARTM",
                "meta_parameters": {
                },
                "hierarchical": False,
                "number_of_topics": number_of_topics,
                "is_actualizable": is_actualizable,
            }
        )
        topic_modelling = DjangoOperator(
            task_id="topic_modelling",
            python_callable=topic_modelling,
            op_kwargs={
                "name": name,
                "corpus": filters['corpus'],
                "regularization_params": regularization_params,
                "is_actualizable": is_actualizable,
            }
        )
        dataset_prepare >> topic_modelling
    if is_actualizable:
        actualizable_bigartms.append(
            {
                "name": name,
                "topic_modelling": topic_modelling,
                "regularization_params": regularization_params,
                "filters": filters
            }
        )


dag1 = DAG('NLPmonitor_BigARTM', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
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
                }, is_actualizable=True)


dag5 = DAG('NLPmonitor_BigARTM_two_years', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag5, name="bigartm_two_years", description="Two last years", number_of_topics=200,
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
                }, is_actualizable=True)


dag6 = DAG('NLPmonitor_BigARTM_education_two_years', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag6, name="bigartm_education_two_years", description="Two last years education", number_of_topics=150,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2017, 11, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 7,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)


dag7 = DAG('NLPmonitor_BigARTM_education_one_year', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag7, name="bigartm_education_one_year", description="One last year education", number_of_topics=100,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2018, 11, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 7,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)


dag8 = DAG('NLPmonitor_BigARTM_education_half_year', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag8, name="bigartm_education_half_year", description="One half year education", number_of_topics=100,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2019, 5, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 7,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)


dag9 = DAG('NLPmonitor_BigARTM_science_two_years', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag9, name="bigartm_science_two_years", description="Two last years science", number_of_topics=150,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2017, 11, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 8,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)


dag10 = DAG('NLPmonitor_BigARTM_science_one_year', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag10, name="bigartm_science_one_year", description="One last year science", number_of_topics=100,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2018, 11, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 8,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)


dag11 = DAG('NLPmonitor_BigARTM_science_half_year', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
gen_bigartm_dag(dag=dag11, name="bigartm_science_half_year", description="One half year science", number_of_topics=100,
                filters={
                    "corpus": "main",
                    "source": None,
                    "datetime_from": date(2019, 5, 1),
                    "datetime_to": date(2019, 12, 1),
                    "group_id": 8,
                    "topic_weight_threshold": 0.05,
                },
                regularization_params={
                    "SmoothSparseThetaRegularizer": 0.15,
                    "SmoothSparsePhiRegularizer": 0.15,
                    "DecorrelatorPhiRegularizer": 0.15,
                    "ImproveCoherencePhiRegularizer": 0.15
                }, is_actualizable=True)
