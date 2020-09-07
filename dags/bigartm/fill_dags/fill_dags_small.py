from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_small(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_small', catchup=False, max_active_runs=1, concurrency=10, default_args=default_args,
               schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        for i in range(10, 301, 10):
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_two_years_{i}", description="Two lyears", number_of_topics=i,
                                 filters={
                                     "corpus": "main",
                                     "source": None,
                                     "datetime_from": date(2017, 11, 1),
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
