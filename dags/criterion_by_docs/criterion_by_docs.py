from datetime import datetime, timedelta

from DjangoOperator import DjangoOperator
from airflow import DAG

from dags.criterion_by_docs.services.criterion_scorer import score_docs

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
    'pool': 'short_tasks'
}

dag = DAG('Criterion_by_docs', catchup=False, max_active_runs=1, default_args=default_args,
          schedule_interval=None)

with dag:
    crit_by_docs = DjangoOperator(
        task_id="criterion_by_docs",
        python_callable=score_docs,
        pool="short_tasks",
        op_kwargs={
            'num_topics': 200,
            'model_name': 'bigartm_two_years',
            'criterion_name': "gosprograms",
            'criterion_name_unicode': 'Гос. программы',
            'docs_folder_name': "docs_gosprograms",
        },
    )
