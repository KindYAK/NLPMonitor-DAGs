"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json
from datetime import datetime, timedelta, date

from DjangoOperator import DjangoOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

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

def gen_bigartm_operator(name, name_translit=None):
    from dags.bigartm_init_unique_indices_to_delete.services.service import init_uniques

    bigartm_calc_operator = DjangoOperator(
        task_id=f"init_uniques_{name_translit if name_translit else name}",
        python_callable=init_uniques,
        op_kwargs={
            "name": name,
        }
    )


groups = json.loads(Variable.get('topic_groups', default_var="[]"))

dag = DAG('NLPmonitor_bigartm_init_unique_indices_to_delete', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)
with dag:
    gen_bigartm_operator(name="bigartm_test")

    gen_bigartm_operator(name="bigartm_two_years")

    gen_bigartm_operator(name="bigartm_education_two_years")

    gen_bigartm_operator(name="bigartm_education_one_year")

    gen_bigartm_operator(name="bigartm_education_half_year")

    gen_bigartm_operator(name="bigartm_science_two_years")

    gen_bigartm_operator(name="bigartm_science_one_year")

    gen_bigartm_operator(name="bigartm_science_half_year")

    # BigARTMs for two_year Zhazira's folders
    groups_bigartm_two_years = filter(lambda x: x['topic_modelling_name'] == "bigartm_two_years", groups)
    for group in groups_bigartm_two_years:
        gen_bigartm_operator(name=f"bigartm_{group['name']}_two_years", name_translit=f"bigartm_{group['name_translit']}_two_years")

    group_info_security = filter(lambda x: x['id'] == 85, groups)
    for group in group_info_security:
        gen_bigartm_operator(name=f"bigartm_{group['name']}_it_two_years", name_translit=f"bigartm_{group['name_translit']}_it_two_years")


    group_info_security = filter(lambda x: x['id'] == 86, groups)
    for group in group_info_security:
        gen_bigartm_operator(name=f"bigartm_{group['name']}_2_level_it_two_years", name_translit=f"bigartm_{group['name_translit']}_2_level_it_two_years")
