import json
from datetime import date

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args


def fill_dags_two_years(actualizable_bigartms, comboable_bigartms):
    dag = DAG('NLPmonitor_BigARTMs_two_years_', catchup=False, max_active_runs=1, concurrency=7,
               default_args=default_args, schedule_interval=None)
    groups = json.loads(Variable.get('topic_groups', default_var="[]"))
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_two_years", description="Two last years", number_of_topics=200,
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
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_two_years_old_parse", description="Two last years old parse",
                             number_of_topics=200,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2017, 6, 1),
                                 "datetime_to": date(2019, 6, 1),
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=False)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_two_years", description="Two last years education",
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
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_one_year", description="One last year education",
                             number_of_topics=100,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2018, 11, 1),
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
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_education_half_year", description="One half year education",
                             number_of_topics=100,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2019, 5, 1),
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
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_science_two_years", description="Two last years science",
                             number_of_topics=150,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2017, 11, 1),
                                 "datetime_to": date(2019, 12, 1),
                                 "group_id": 8,
                                 "topic_weight_threshold": 0.05,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_science_one_year", description="One last year science", number_of_topics=100,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2018, 11, 1),
                                 "datetime_to": date(2019, 12, 1),
                                 "group_id": 8,
                                 "topic_weight_threshold": 0.05,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name="bigartm_science_half_year", description="One half year science",
                             number_of_topics=100,
                             filters={
                                 "corpus": "main",
                                 "source": None,
                                 "datetime_from": date(2019, 5, 1),
                                 "datetime_to": date(2019, 12, 1),
                                 "group_id": 8,
                                 "topic_weight_threshold": 0.05,
                             },
                             regularization_params={
                                 "SmoothSparseThetaRegularizer": 0.15,
                                 "SmoothSparsePhiRegularizer": 0.15,
                                 "DecorrelatorPhiRegularizer": 0.15,
                                 "ImproveCoherencePhiRegularizer": 0.15
                             },
                             wait_for_basic_tms=wait_for_basic_tms,
                             is_actualizable=True)

        # BigARTMs for two_year Zhazira's folders
        groups_bigartm_two_years = filter(lambda x: x['topic_modelling_name'] == "bigartm_two_years", groups)
        for group in groups_bigartm_two_years:
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_{group['name']}_two_years", description=f"Two years {group['name']}",
                                 number_of_topics=100,
                                 filters={
                                     "corpus": "main",
                                     "source": None,
                                     "datetime_from": date(2010, 5, 1),
                                     "datetime_to": date(2020, 1, 1),
                                     "group_id": group['id'],
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
                                 name_translit=f"bigartm_{group['name_translit']}_two_years",
                                 topic_modelling_translit=group['topic_modelling_name_translit'],
                                 )

        group_info_security = filter(lambda x: x['id'] == 85, groups)
        for group in group_info_security:
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_{group['name']}_it_two_years",
                                 description=f"IT two years {group['name']}",
                                 number_of_topics=50,
                                 filters={
                                     "corpus": "main",
                                     "source": None,
                                     "datetime_from": date(2010, 5, 1),
                                     "datetime_to": date(2020, 1, 1),
                                     "group_id": group['id'],
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
                                 name_translit=f"bigartm_{group['name_translit']}_it_two_years",
                                 topic_modelling_translit=group['topic_modelling_name_translit'],
                                 )

        group_info_security = filter(lambda x: x['id'] == 86, groups)
        for group in group_info_security:
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm_{group['name']}_2_level_it_two_years",
                                 description=f"IT two 2 level years {group['name']}",
                                 number_of_topics=25,
                                 filters={
                                     "corpus": "main",
                                     "source": None,
                                     "datetime_from": date(2010, 5, 1),
                                     "datetime_to": date(2020, 1, 1),
                                     "group_id": group['id'],
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
                                 name_translit=f"bigartm_{group['name_translit']}_2_level_it_two_years",
                                 topic_modelling_translit=group['topic_modelling_name_translit'],
                                 )
    return dag
