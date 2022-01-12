from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_news_and_gos(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_news_and_gos', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_main_and_gos", description="Main and gos 2 yearts",
                             number_of_topics=200,
                             filters={
                                 "corpus": ["main", "gos"],
                                 "corpus_datetime_ignore": ["gos"],
                                 "source": None,
                                 "datetime_from": date(2018, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_main_and_gos2", description="Main and gos2 2 yearts",
                             number_of_topics=200,
                             filters={
                                 "corpus": ["main", "gos2"],
                                 "corpus_datetime_ignore": ["gos2"],
                                 "source": None,
                                 "datetime_from": date(2018, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_1000_main_and_gos2", description="Main and gos2 2 years, 1000 topics",
                             number_of_topics=1000,
                             filters={
                                 "corpus": ["main", "gos2"],
                                 "corpus_datetime_ignore": ["gos2"],
                                 "source": None,
                                 "datetime_from": date(2018, 1, 1),
                                 "datetime_to": date(2020, 6, 10),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)
    return dag
