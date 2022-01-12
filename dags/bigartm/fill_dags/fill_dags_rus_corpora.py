from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_rus_corpora(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_rus', catchup=False, max_active_runs=1, concurrency=7, default_args=default_args,
               schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_full_lenta", description="Lenta full", number_of_topics=250,
                             filters={
                                 "corpus": "rus",
                                 "source": None,
                                 "datetime_from": date(2000, 1, 1),
                                 "datetime_to": date(2020, 5, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_lenta", description="Lenta full", number_of_topics=200,
                             filters={
                                 "corpus": "rus",
                                 "source": None,
                                 "datetime_from": date(2018, 1, 1),
                                 "datetime_to": date(2020, 5, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        # ############### rus vs rus_propaganda #######################
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_full_rus_and_rus_propaganda", description="",
                             number_of_topics=250,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2000, 1, 1),
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_rus_and_rus_propaganda", description="",
                             number_of_topics=200,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_1000_rus_and_rus_propaganda", description="",
                             number_of_topics=1000,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2018, 2, 1),
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_2020_rus_and_rus_propaganda", description="",
                             number_of_topics=150,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
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

        # ############### rus vs kz #######################
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_full_rus_and_main", description="",
                             number_of_topics=250,
                             filters={
                                 "corpus": ["rus", "main"],
                                 "source": None,
                                 "datetime_from": date(2000, 1, 1),
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_rus_and_main", description="",
                             number_of_topics=200,
                             filters={
                                 "corpus": ["rus", "main"],
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_2020_rus_and_main", description="",
                             number_of_topics=150,
                             filters={
                                 "corpus": ["rus", "main"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
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

        # ############### rus_propaganda vs kz #######################
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_full_rus_propaganda_and_main", description="",
                             number_of_topics=250,
                             filters={
                                 "corpus": ["rus_propaganda", "main"],
                                 "source": None,
                                 "datetime_from": date(2000, 1, 1),
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_rus_propaganda_and_main", description="",
                             number_of_topics=200,
                             filters={
                                 "corpus": ["rus_propaganda", "main"],
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

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_2020_rus_propaganda_and_main", description="",
                             number_of_topics=150,
                             filters={
                                 "corpus": ["rus_propaganda", "main"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
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
    return dag
