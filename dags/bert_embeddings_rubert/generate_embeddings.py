from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator
# from airflow.operators.python_operator import PythonVirtualenvOperator
# from PythonVirtualenvCachedOperator import PythonVirtualenvCachedOperator


def test_connections_to_bert_service(created):
    print(f'starting task at {created}')
    from bert_serving.client import BertClient

    bc = BertClient(ip="bert_as_service")
    vec = bc.encode(['First do it', 'then do it right', 'then do it better'])
    print('-'*10)
    print(vec.shape)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 3),
    'email': ['bekbaganbetov.abay@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 99,
    'pool': 'short_tasks',
}


dag = DAG(
    'Nlpmonitor_generate_rubert_embeddings', catchup=False, max_active_runs=1,
    default_args=default_args, schedule_interval=None
)

with dag:
    # Word
    init_word_index = DjangoOperator(
        task_id="test_connections_to_bert_service",
        python_callable=test_connections_to_bert_service,
        pool="short_tasks",
        op_kwargs={
            "created": datetime.now(),
        }
    )
