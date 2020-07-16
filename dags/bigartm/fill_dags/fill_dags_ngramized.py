from datetime import date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_ngramized(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_ngramized', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_test_ngram", description="Two last years", number_of_topics=200,
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
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_ngrams_dict_pymorphy_2_4_393442_3710985",
                             )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_two_years_ngram", description="Two last years", number_of_topics=200,
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
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_ngrams_dict_pymorphy_2_4_393442_3710985",
                             )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_two_years_ngram", description="Two last years education",
                             number_of_topics=150,
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
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_ngrams_dict_pymorphy_2_4_393442_3710985",
                             )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_hate", description="Hate speech dataset",
                             number_of_topics=75,
                             filters={
                                 "corpus": "hate",
                                 "source": None,
                                 "datetime_from": None,
                                 "datetime_to": None,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.25,
                                 "DecorrelatorPhiRegularizer": 0.3,
                                 "ImproveCoherencePhiRegularizer": 0.2
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False,
                             text_field="text_ngramized_en_lemminflect",
                             )

    dag8 = DAG('NLPmonitor_BigARTMs_ngramized_yandex', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    with dag8:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_test_ngram_yandex", description="Two last years", number_of_topics=200,
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
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict",
                             )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_two_years_ngram_yandex", description="Two last years", number_of_topics=200,
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
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict",
                             )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_two_years_ngram_yandex", description="Two last years education",
                             number_of_topics=150,
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
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True,
                             text_field="text_ngramized_kz_rus_yandex_ngrams_dict",
                             )
    return dag
