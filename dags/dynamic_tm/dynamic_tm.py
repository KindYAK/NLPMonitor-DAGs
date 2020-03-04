from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.dynamic_tm.services.meta_dtm_creator import generate_meta_dtm

from_date = '2019-01-01'
to_date = '2019-04-01'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 50,
    'pool': 'long_tasks'
}

dynamic_tm_parameters = {
    'name': "dynamic_tm_test",
    'name_immutable': "dynamic_tm_test",
    'description': "All news",
    'number_of_topics': 100,
    'filters': {
        "corpus": "main",
        "source": None,
        "datetime_from": None,
        "datetime_to": None,
    },
    'regularization_params': {
        "SmoothSparseThetaRegularizer": 0.15,
        "SmoothSparsePhiRegularizer": 0.15,
        "DecorrelatorPhiRegularizer": 0.15,
        "ImproveCoherencePhiRegularizer": 0.15
    },
    'is_actualizable': False,
    'name_translit': None,
    'topic_modelling_translit': None

}

meta_dtm_parameters = {
    'meta_dtm_name': f"meta_dtm_{from_date}_{to_date}",
    'tm_volume_days': 30,
    'delta_days': 15,
    'reset_index': True,
    'from_date': from_date,
    'to_date': to_date
}

mapping_parameters = {
    'name': dynamic_tm_parameters['name'],
    'name_immutable': dynamic_tm_parameters['name_immutable'],
    'meta_dtm_name': meta_dtm_parameters['meta_dtm_name'],
    'number_of_topics': dynamic_tm_parameters['number_of_topics'],
    'datetime_from_tm_1': None,  # update in cycle
    'datetime_to_tm_1': None,
    'datetime_from_tm_2': None,
    'datetime_to_tm_2': None,
}

dynamic_tm_calc_operators = []
dynamic_mapper_operators = []


def gen_dynamic_tm_operator(**kwargs):
    description = kwargs['description']
    number_of_topics = kwargs['number_of_topics']
    filters = kwargs['filters']
    name = kwargs['name']
    regularization_params = kwargs['regularization_params']
    is_actualizable = kwargs['is_actualizable']
    name_translit = kwargs['name_translit']
    topic_modelling_translit = kwargs['topic_modelling_translit']
    meta_dtm_name = kwargs['meta_dtm_name']

    from dags.bigartm.services.service import bigartm_calc

    if not name_translit:
        task_id = f"dynamic_tm_calc_{name}_{filters['datetime_from'].date()}_{filters['datetime_to'].date()}"
    else:
        task_id = f"dynamic_tm_calc_{topic_modelling_translit}_{name_translit}_{filters['datetime_from'].date()}_{filters['datetime_to'].date()}"
    dynamic_tm_calc_operator = DjangoOperator(
        task_id=task_id,
        python_callable=bigartm_calc,
        op_kwargs={
            "name": name,
            "name_translit": name_translit,
            "corpus": filters['corpus'],
            "source": filters['source'],
            "datetime_from": filters['datetime_from'],
            "datetime_to": filters['datetime_to'],
            "group_id": filters['group_id'] if 'group_id' in filters else None,
            "topic_weight_threshold": filters[
                'topic_weight_threshold'] if 'topic_weight_threshold' in filters else 0.05,
            "is_ready": False,
            "description": description,
            "datetime_created": datetime.now(),
            "algorithm": "BigARTM",
            "meta_parameters": {

            },
            "number_of_topics": number_of_topics,
            "regularization_params": regularization_params,
            "is_actualizable": is_actualizable,
            "is_dynamic": True,
            "meta_dtm_name": meta_dtm_name
        }
    )
    dynamic_tm_calc_operators.append(dynamic_tm_calc_operator)


def gen_mapper_operator(**kwargs):
    from dags.dynamic_tm.services.tms_mapper import mapper
    task_id = 'mapping_between_tm1_' + str(kwargs['datetime_from_tm_1'].date()) + "_tm2_" + str(
        kwargs['datetime_from_tm_2'].date())
    mapper_operator = DjangoOperator(
        task_id=task_id,
        python_callable=mapper,
        op_kwargs=kwargs
    )
    dynamic_mapper_operators.append(mapper_operator)


dag = DAG('NLPmonitor_Dynamic_BigARTMs', catchup=False, max_active_runs=1, default_args=default_args,
          schedule_interval=None)


def gen_meta_tdm_operator(from_date, to_date, mydag, meta_dtm_parameters, dynamic_tm_parameters, mapping_parameters):
    with mydag:
        from datetime import datetime as dt
        from datetime import timedelta

        from_date = dt.strptime(from_date, '%Y-%m-%d')
        to_date = dt.strptime(to_date, '%Y-%m-%d')

        delta_days = meta_dtm_parameters['delta_days']
        meta_dtm_name = meta_dtm_parameters['meta_dtm_name']
        tm_volume_days = meta_dtm_parameters['tm_volume_days']

        date_iterations = (to_date - from_date) / timedelta(days=delta_days)

        meta_dtm = DjangoOperator(task_id=f"meta_dtm_creating_{from_date.date()}_{to_date.date()}",
                                  python_callable=generate_meta_dtm,
                                  op_kwargs=meta_dtm_parameters)

        dynamic_tm_parameters['meta_dtm_name'] = meta_dtm_name
        for iteration in range(int(date_iterations) - 1):
            from_d = from_date + timedelta(days=delta_days * iteration)
            to_d = from_d + timedelta(days=tm_volume_days)
            if iteration == int(date_iterations) - 1:
                to_d = to_date

            print(f'Iteration num: {iteration}  | from: {from_d} to: {to_d}')

            dynamic_tm_parameters['filters']['datetime_from'] = from_d
            dynamic_tm_parameters['filters']['datetime_to'] = to_d

            if not iteration:  # initiate from for 1st(0) iteration
                gen_dynamic_tm_operator(**dynamic_tm_parameters)
                mapping_parameters['datetime_from_tm_1'] = from_d
                mapping_parameters['datetime_to_tm_1'] = to_d
                continue

            gen_dynamic_tm_operator(**dynamic_tm_parameters)
            mapping_parameters['datetime_from_tm_2'] = from_d
            mapping_parameters['datetime_to_tm_2'] = to_d
            mapping_parameters['name'] = mapping_parameters['name'] + "_" + str(
                mapping_parameters['datetime_from_tm_1'].date()) + "_" + str(
                mapping_parameters['datetime_to_tm_1'].date()) + "_" + str(
                mapping_parameters['datetime_from_tm_2'].date()) + "_" + str(
                mapping_parameters['datetime_to_tm_2'].date())

            gen_mapper_operator(**mapping_parameters)
            meta_dtm >> dynamic_tm_calc_operators[-2:] >> dynamic_mapper_operators[-1]

            mapping_parameters['datetime_from_tm_1'] = from_d  # replacing previous from to date to current
            mapping_parameters['datetime_to_tm_1'] = to_d  # || - || - || - ||


gen_meta_tdm_operator(from_date=from_date,
                      to_date=to_date,
                      mydag=dag,
                      meta_dtm_parameters=meta_dtm_parameters,
                      dynamic_tm_parameters=dynamic_tm_parameters,
                      mapping_parameters=mapping_parameters)
