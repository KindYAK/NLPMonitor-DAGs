def fill_dags_scopus(actualizable_bigartms, comboable_bigartms):
    import json
    from datetime import date

    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.python_operator import PythonOperator

    from dags.bigartm.fill_dags.utils import gen_bigartm_operator, default_args

    dag = DAG('NLPmonitor_BigARTMs_Scopus', catchup=False, max_active_runs=1, concurrency=1,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )
        for num_topics in [100, 500]:
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm__scopus_{num_topics}", description=f"scopus {num_topics} topics",
                                 number_of_topics=num_topics,
                                 filters={
                                     "corpus": "scopus_real_real",
                                     "corpus_datetime_ignore": ["scopus_real_real"],
                                     "source": None,
                                     "datetime_from": date(1900, 1, 1),
                                     "datetime_to": date(2050, 1, 1),
                                 },
                                 regularization_params={
                                     "SmoothSparseThetaRegularizer": 0.15,
                                     "SmoothSparsePhiRegularizer": 0.15,
                                     "DecorrelatorPhiRegularizer": 0.15,
                                     "ImproveCoherencePhiRegularizer": 0.15
                                 },
                                 wait_for_basic_tms=wait_for_basic_tms,
                                 is_actualizable=False,
                                 text_field="text_ngramized_en_scopus_extend"
                                 )

    groups = json.loads(Variable.get('topic_groups', default_var="[]"))
    dag = DAG('NLPmonitor_BigARTMs_Scopus_hierarchy', catchup=False, max_active_runs=1, concurrency=5,
               default_args=default_args, schedule_interval=None)
    with dag:
        wait_for_basic_tms = PythonOperator(
            task_id="wait_for_basic_tms",
            python_callable=lambda: 0,
        )
        for group in filter(lambda x: x['topic_modelling_name'] == "bigartm__scopus_100", groups):
            gen_bigartm_operator(actualizable_bigartms, comboable_bigartms, name=f"bigartm__scopus_100_{group['name_translit']}", description=f"scopus 100 topics hierarchy, {group['name_translit']}",
                                 number_of_topics=50,
                                 filters={
                                     "corpus": "scopus_real_real",
                                     "corpus_datetime_ignore": ["scopus_real_real"],
                                     "source": None,
                                     "datetime_from": date(1900, 1, 1),
                                     "datetime_to": date(2050, 1, 1),
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
                                 is_actualizable=False,
                                 text_field="text_ngramized_en_scopus_extend"
                                 )

    return dag
