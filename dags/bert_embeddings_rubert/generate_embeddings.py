from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator


def init_process(created):
    print(created)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 3),
    'email': ['bekbaganbetov.abay@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 20,
    'pool': 'short_tasks',
}


dag = DAG(
    'Nlpmonitor_generate_rubert_embeddings', catchup=False, max_active_runs=1,
    default_args=default_args, schedule_interval=None
)

with dag:
    # Word
    init_word_index = DjangoOperator(
        task_id="init_word_index",
        python_callable=init_process,
        pool="short_tasks",
        op_kwargs={
            "created": datetime.now(),
        }
    )
