from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_healthcare(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_Healthcare', catchup=False, max_active_runs=1, concurrency=7, default_args=default_args,
               schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        # ZERO LEVEL
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_kaz", description="2020-2021", number_of_topics=200,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")


        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus", description="2020-2021", number_of_topics=200,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")


        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_kaz", description="2020-2021", number_of_topics=200,
                             filters={
                                 "corpus": ["main", "rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")


        # FIRST LEVEL
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_health_1",
                             description="2020-2021", number_of_topics=150,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 101,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_kaz_health_1",
                             description="2020-2021", number_of_topics=150,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 102,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_kaz_health_1",
                             description="2020-2021", number_of_topics=150,
                             filters={
                                 "corpus": ["main", "rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 104,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        # SECOND LEVEL
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_health_2",
                             description="2020-2021", number_of_topics=100,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 105,
                                 "topic_weight_threshold": 0.04,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_kaz_health_2",
                             description="2020-2021", number_of_topics=100,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 106,
                                 "topic_weight_threshold": 0.04,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_kaz_health_2",
                             description="2020-2021", number_of_topics=100,
                             filters={
                                 "corpus": ["main", "rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 15),
                                 "group_id": 107,
                                 "topic_weight_threshold": 0.04,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        # SECOND LEVEL
        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_health_3",
                             description="2020-2021", number_of_topics=50,
                             filters={
                                 "corpus": ["rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 20),
                                 "group_id": 108,
                                 "topic_weight_threshold": 0.1,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_kaz_health_3",
                             description="2020-2021", number_of_topics=50,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 20),
                                 "group_id": 109,
                                 "topic_weight_threshold": 0.1,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_2020_2021_rus_kaz_health_3",
                             description="2020-2021", number_of_topics=50,
                             filters={
                                 "corpus": ["main", "rus", "rus_propaganda"],
                                 "source": None,
                                 "datetime_from": date(2020, 1, 1),
                                 "datetime_to": date(2021, 4, 20),
                                 "group_id": 110,
                                 "topic_weight_threshold": 0.1,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict")

    return dag
