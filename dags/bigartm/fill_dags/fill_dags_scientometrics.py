from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_scientometrics(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_scientometrics', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_scientometrics_15", description="scientometrics 17k 15 topics",
                             number_of_topics=15,
                             filters={
                                 "corpus": "scientometrics",
                                 "source": None,
                                 "datetime_from": date(2004, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_scientometrics_25", description="scientometrics 17k 25 topics",
                             number_of_topics=25,
                             filters={
                                 "corpus": "scientometrics",
                                 "source": None,
                                 "datetime_from": date(2004, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_scientometrics_50", description="scientometrics 17k 50 topics",
                             number_of_topics=50,
                             filters={
                                 "corpus": "scientometrics",
                                 "source": None,
                                 "datetime_from": date(2004, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_scientometrics_75", description="scientometrics 17k 75 topics",
                             number_of_topics=75,
                             filters={
                                 "corpus": "scientometrics",
                                 "source": None,
                                 "datetime_from": date(2004, 1, 1),
                                 "datetime_to": date(2020, 4, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)
    return dag
