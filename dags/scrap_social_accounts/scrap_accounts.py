from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.scrap_social_accounts.scrap_ig_accounts.scrap_ig import create_ig_accounts

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=30),
    'priority_weight': 90,
    'pool': 'short_tasks'
}

dag = DAG('Get_ig_accs', catchup=False, max_active_runs=1, default_args=default_args, schedule_interval=None)

accounts = list()

with dag:
    accounts.append(DjangoOperator(
        task_id=f"create_instagram_accounts",
        python_callable=create_ig_accounts,
        op_kwargs={
            "username": "tengrinewskz",
            "num_followers": 10_000,
        }
    ))
