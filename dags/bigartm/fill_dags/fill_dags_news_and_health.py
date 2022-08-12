from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_news_and_health(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_codex', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_main_and_health", description="Main and gos 2 yearts",
                             number_of_topics=300,
                             filters={
                                 "corpus": ["main", "codex_health"],
                                 "corpus_datetime_ignore": ["codex_health"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2022, 7, 28),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_main_and_health_only_old", description="Main and gos 2 yearts",
                             number_of_topics=300,
                             filters={
                                 "corpus": ["main", "codex_health"],
                                 "corpus_datetime_ignore": ["codex_health"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2022, 3, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        # gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_health_news_and_health",
        #                      description="Main and gos 2 yearts",
        #                      number_of_topics=150,
        #                      filters={
        #                          "corpus": ["main", "codex_health"],
        #                          "corpus_datetime_ignore": ["codex_health"],
        #                          "source": None,
        #                          "datetime_from": date(2020, 1, 1),
        #                          "datetime_to": date(2022, 7, 28),
        #                          "group_id": 213,
        #                      },
        #                      regularization_params={
        #                          "SmoothSparseThetaRegularizer": 0.15,
        #                          "SmoothSparsePhiRegularizer": 0.15,
        #                          "DecorrelatorPhiRegularizer": 0.15,
        #                          "ImproveCoherencePhiRegularizer": 0.15
        #                      },
        #                      wait_for_basic_tms=wait_for_basic_tms,
        #                      is_actualizable=False)
    return dag
