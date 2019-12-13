"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from airflow.operators.python_operator import PythonOperator
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
bigartm_calc_operators = []
def gen_bigartm_dag(name, description, number_of_topics, filters, regularization_params, is_actualizable=False):
    from dags.bigartm.services.service import bigartm_calc

    bigartm_calc_operator = DjangoOperator(
        task_id=f"bigartm_calc_{name}",
        python_callable=bigartm_calc,
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
            "regularization_params": regularization_params,
            "is_actualizable": is_actualizable,
        }
    )
    bigartm_calc_operators.append(bigartm_calc_operator)
    if 'group_id' in filters and filters['group_id']:
        wait_for_basic_tms >> bigartm_calc_operator
    else:
        bigartm_calc_operator >> wait_for_basic_tms
    if is_actualizable:
        actualizable_bigartms.append(
            {
                "name": name,
                "regularization_params": regularization_params,
                "filters": filters
            }
        )


dag = DAG('NLPmonitor_BigARTMs', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
with dag:
    wait_for_basic_tms = PythonOperator(
        task_id="wait_for_basic_tms",
        python_callable=lambda: 0,
    )

    gen_bigartm_dag(name="bigartm_test", description="All news", number_of_topics=250,
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

    gen_bigartm_dag(name="bigartm_two_years", description="Two last years", number_of_topics=200,
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

    gen_bigartm_dag(name="bigartm_education_two_years", description="Two last years education", number_of_topics=150,
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

    gen_bigartm_dag(name="bigartm_education_one_year", description="One last year education", number_of_topics=100,
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

    gen_bigartm_dag(name="bigartm_education_half_year", description="One half year education", number_of_topics=100,
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

    gen_bigartm_dag(name="bigartm_science_two_years", description="Two last years science", number_of_topics=150,
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

    gen_bigartm_dag(name="bigartm_science_one_year", description="One last year science", number_of_topics=100,
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

    gen_bigartm_dag(name="bigartm_science_half_year", description="One half year science", number_of_topics=100,
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

    # BigARTMs for two_year Zhazira's folders

