from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator

from bert_embeddings.gen_embed.services import init_token_embedding_index, generate_token_embeddings, persist_token_embeddings


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 8),
    'email': ['yakunin.k@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG('generate_bert_embeddings', default_args=default_args, schedule_interval=None)

with dag:
    init_index = DjangoOperator(
        task_id="init_index",
        python_callable=init_token_embedding_index,
    )

    token_embedding_operators = []
    concurrency = 8
    for i in range(concurrency):
        token_embedding_operators.append(DjangoOperator(
            task_id=f"gen_token_embedding_{i}",
            python_callable=generate_token_embeddings,
            op_kwargs={"start": (100/concurrency)*i,
                       "end": (100/concurrency)*(i+1)}
        ))

    persist_token_embeddings = DjangoOperator(
        task_id="persist_token_embeddings",
        python_callable=persist_token_embeddings,
    )

    init_index >> token_embedding_operators >> persist_token_embeddings
