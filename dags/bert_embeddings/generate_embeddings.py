from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator

from dags.bert_embeddings.gen_embed.services import init_token_embedding_index, generate_token_embeddings, persist_token_embeddings

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

dag = DAG('NLPMonitor_generate_bert_embeddings', default_args=default_args, schedule_interval=None)

with dag:
    # Token
    init_token_index = DjangoOperator(
        task_id="init_token_index",
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

    init_token_index >> token_embedding_operators >> persist_token_embeddings

    def not_implemented():
        raise Exception("Not implemented")

    # Word
    init_word_index = DjangoOperator(
        task_id="init_word_average_index",
        python_callable=not_implemented,
    )

    word_embedding_operators = []
    concurrency = 8
    for i in range(concurrency):
        word_embedding_operators.append(DjangoOperator(
            task_id=f"gen_word_average_embedding_{i}",
            python_callable=not_implemented,
            op_kwargs={"start": (100 / concurrency) * i,
                       "end": (100 / concurrency) * (i + 1)}
        ))

    persist_word_embeddings = DjangoOperator(
        task_id="persist_word_average_embeddings",
        python_callable=not_implemented,
    )

    token_embedding_operators >> init_word_index >> word_embedding_operators >> persist_word_embeddings

    # Sentence
    init_sentence_index = DjangoOperator(
        task_id="init_sentence_average_index",
        python_callable=not_implemented,
    )

    sentence_embedding_operators = []
    concurrency = 8
    for i in range(concurrency):
        sentence_embedding_operators.append(DjangoOperator(
            task_id=f"gen_sentence_average_embedding_{i}",
            python_callable=not_implemented,
            op_kwargs={"start": (100 / concurrency) * i,
                       "end": (100 / concurrency) * (i + 1)}
        ))

    persist_sentence_embeddings = DjangoOperator(
        task_id="persist_sentence_average_embeddings",
        python_callable=not_implemented,
    )

    word_embedding_operators >> init_sentence_index >> sentence_embedding_operators >> persist_sentence_embeddings

    # Text
    init_text_index = DjangoOperator(
        task_id="init_text_average_max_index",
        python_callable=not_implemented,
    )

    text_embedding_operators = []
    concurrency = 8
    for i in range(concurrency):
        text_embedding_operators.append(DjangoOperator(
            task_id=f"gen_text_average_max_embedding_{i}",
            python_callable=not_implemented,
            op_kwargs={"start": (100 / concurrency) * i,
                       "end": (100 / concurrency) * (i + 1)}
        ))

    persist_text_embeddings = DjangoOperator(
        task_id="persist_text_average_max_embeddings",
        python_callable=not_implemented,
    )

    sentence_embedding_operators >> init_text_index >> text_embedding_operators >> persist_text_embeddings
