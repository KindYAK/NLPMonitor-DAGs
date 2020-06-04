from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_full(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_full', catchup=False, max_active_runs=1, concurrency=7, default_args=default_args,
              schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_test", description="All news", number_of_topics=250,
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
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)
    return dag
