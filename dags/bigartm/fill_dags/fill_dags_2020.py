from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_2020(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_2020', catchup=False, max_active_runs=1, concurrency=7, default_args=default_args,
               schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020", description="2020", number_of_topics=175,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2020, 12, 31),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        # gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_2019", description="2019 education", number_of_topics=90,
        #                      filters={
        #                          "corpus": "main",
        #                          "source": None,
        #                          "datetime_from": date(2019, 1, 1),
        #                          "datetime_to": date(2019, 12, 31),
        #                          "group_id": 87,
        #                          "topic_weight_threshold": 0.04,
        #                      },
        #                      regularization_params={
        #                          "SmoothSparseThetaRegularizer": 0.15,
        #                          "SmoothSparsePhiRegularizer": 0.15,
        #                          "DecorrelatorPhiRegularizer": 0.15,
        #                          "ImproveCoherencePhiRegularizer": 0.15
        #                      },
        #                      wait_for_basic_tms=wait_for_basic_tms,
        #                      is_actualizable=False)
        #
        # gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_2_2019", description="2019 education 2 distilled",
        #                      number_of_topics=125,
        #                      filters={
        #                          "corpus": "main",
        #                          "source": None,
        #                          "datetime_from": date(2019, 1, 1),
        #                          "datetime_to": date(2019, 12, 31),
        #                          "group_id": 93,
        #                          "topic_weight_threshold": 0.025,
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
