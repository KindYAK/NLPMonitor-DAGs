"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import json

from airflow import DAG
from airflow.models import Variable
from DjangoOperator import DjangoOperator
from datetime import datetime, timedelta

from dags.scraper_social.scrap.service import scrap_wrapper


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 50,
    'pool': 'scraper_tasks',
    'queue': 'second', # TODO Uncomment before commit, comment durig development
}

default_args_low = default_args.copy()
default_args_low['priority_weight'] = 40
default_args_low['retry_delay'] = timedelta(minutes=30)

default_args_medium = default_args.copy()
default_args_medium['priority_weight'] = 50
default_args_medium['retry_delay'] = timedelta(minutes=10)

default_args_high = default_args.copy()
default_args_high['priority_weight'] = 60
default_args_high['retry_delay'] = timedelta(minutes=1)

accounts = json.loads(Variable.get('social_accounts', default_var="[]"))
networks = json.loads(Variable.get('social_networks', default_var="[]"))

dag_low = DAG('Scrapers_scrap_social_low', catchup=False, max_active_runs=1, default_args=default_args_low, schedule_interval='15 6 * * 6')
dag_medium = DAG('Scrapers_scrap_social_medium', catchup=False, max_active_runs=1, default_args=default_args_low, schedule_interval='15 12 * * *')
dag_high = DAG('Scrapers_scrap_social_high', catchup=False, max_active_runs=1, default_args=default_args_low, schedule_interval='30 * * * *')

scrapers_low = []
scrapers_medium = []
scrapers_high = []
for social_network in networks:
    # Low
    with dag_low:
        scraper = DjangoOperator(
            task_id=f"scrap_{social_network['name']}_by_account_low",
            python_callable=scrap_wrapper,
            op_kwargs={
                "social_network": social_network['id'],
                "accounts": filter(lambda x: x['social_network'] == social_network['id'] and x['priority_rate'] <= 25, accounts),
            }
        )
    scrapers_low.append(scraper)

    # Medium
    with dag_medium:
        scraper = DjangoOperator(
            task_id=f"scrap_{social_network['name']}_by_account_medium",
            python_callable=scrap_wrapper,
            op_kwargs={
                "social_network": social_network['id'],
                "accounts": filter(lambda x: x['social_network'] == social_network['id'] and 25 < x['priority_rate'] < 75, accounts),
            }
        )
    scrapers_medium.append(scraper)

    # High
    with dag_high:
        scraper = DjangoOperator(
            task_id=f"scrap_{social_network['name']}_by_account_high",
            python_callable=scrap_wrapper,
            op_kwargs={
                "social_network": social_network['id'],
                "accounts": filter(lambda x: x['social_network'] == social_network['id'] and x['priority_rate'] >= 75, accounts),
            }
        )
    scrapers_high.append(scraper)
