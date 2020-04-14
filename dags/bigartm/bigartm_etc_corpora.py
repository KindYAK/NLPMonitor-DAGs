from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.bigartm import default_args, gen_bigartm_operator


dag7 = DAG('NLPmonitor_BigARTMs_rus', catchup=False, max_active_runs=1, concurrency=7, default_args=default_args, schedule_interval=None)
with dag7:
    wait_for_basic_tms = PythonOperator(
        task_id="wait_for_basic_tms",
        python_callable=lambda: 0,
    )
    gen_bigartm_operator(name=f"bigartm_full_lenta", description="Lenta full", number_of_topics=250,
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
                         is_actualizable=True)

    gen_bigartm_operator(name=f"bigartm_two_years_lenta", description="Lenta full", number_of_topics=200,
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
                         is_actualizable=True)
