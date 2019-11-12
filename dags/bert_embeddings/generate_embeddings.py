from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator

from util.util import not_implemented
from dags.bert_embeddings.gen_embed.services import (init_embedding_index,
                                                     generate_word_embeddings,
                                                     persist_embeddings,
                                                     pool_embeddings,
                                                     WORD_EMBEDDING_NAME, SENTENCE_EMBEDDING_NAME, TEXT_EMBEDDING_NAME
                                                     )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 8),
    'email': ['yakunin.k@mail.ru'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=60),
    'priority_weight': 40,
    'pool': 'long_tasks',
}

dag = DAG('NLPMonitor_generate_bert_embeddings', default_args=default_args, schedule_interval=None)

with dag:
    # Word
    init_word_index = DjangoOperator(
        task_id="init_word_index",
        python_callable=init_embedding_index,
        pool="short_tasks",
        op_kwargs={
            "corpus": "main",
            "is_ready": False,
            "name": WORD_EMBEDDING_NAME,
            "description": "Bert word embedding using Rubert model from DeepPavlov",
            "datetime_created": datetime.now(),
            "by_unit": "word",
            "algorithm": "BERT",
            "pooling": "None",
            "meta_parameters": {
                "pre_trained": "rubert_cased_L-12_H-768_A-12_v1",
            },
        }
    )

    word_embedding_operators = []
    concurrency = 512
    for i in range(concurrency):
        word_embedding_operators.append(DjangoOperator(
            task_id=f"gen_word_embedding_{i}",
            python_callable=generate_word_embeddings,
            op_kwargs={
                "start": (100 / concurrency) * i,
                "end": (100 / concurrency) * (i + 1)
            }
        ))

    persist_word_embeddings = DjangoOperator(
        task_id="persist_word_embeddings",
        python_callable=persist_embeddings,
        op_kwargs={
            "corpus": "main",
            "embedding_name": WORD_EMBEDDING_NAME,
            "by_unit": "word",
            "type_unit_int": 0,
            "algorithm": "bert",
            "pooling": "None",
            "description": "Bert word embedding using Rubert model from DeepPavlov"
        }
    )

    init_word_index >> word_embedding_operators >> persist_word_embeddings

    # Sentence
    init_sentence_index = DjangoOperator(
        task_id="init_sentence_average_index",
        python_callable=init_embedding_index,
        pool="short_tasks",
        op_kwargs={
            "corpus": "main",
            "is_ready": False,
            "name": SENTENCE_EMBEDDING_NAME,
            "description": "Bert sentence average pooling embedding using Rubert model from DeepPavlov",
            "datetime_created": datetime.now(),
            "by_unit": "sentence",
            "algorithm": "BERT",
            "pooling": "Average",
            "meta_parameters": {
                "pre_trained": "rubert_cased_L-12_H-768_A-12_v1",
            },
        }
    )

    sentence_embedding_operators = []
    concurrency = 64
    for i in range(concurrency):
        sentence_embedding_operators.append(DjangoOperator(
            task_id=f"gen_sentence_average_embedding_{i}",
            python_callable=pool_embeddings,
            op_kwargs={"start": (100 / concurrency) * i,
                       "end": (100 / concurrency) * (i + 1),
                       "corpus": "main",
                       "from_embedding_name": WORD_EMBEDDING_NAME,
                       "from_embedding_by_unit": "word",
                       "to_embedding_name": SENTENCE_EMBEDDING_NAME,
                       "to_embedding_by_unit": "sentence",
                       "pooling": "Average",
                       }
        ))

    persist_sentence_embeddings = DjangoOperator(
        task_id="persist_sentence_average_embeddings",
        python_callable=persist_embeddings,
        op_kwargs={
            "corpus": "main",
            "embedding_name": SENTENCE_EMBEDDING_NAME,
            "by_unit": "sentence",
            "type_unit_int": 3,
            "algorithm": "bert",
            "pooling": "Average",
            "description": "Bert sentence average pooling embedding using Rubert model from DeepPavlov"
        }
    )

    word_embedding_operators >> init_sentence_index >> sentence_embedding_operators >> persist_sentence_embeddings

    # Text
    init_text_index = DjangoOperator(
        task_id="init_text_average_max_index",
        python_callable=init_embedding_index,
        pool="short_tasks",
        op_kwargs={
            "corpus": "main",
            "is_ready": False,
            "name": TEXT_EMBEDDING_NAME,
            "description": "Bert text average+max pooling embedding using Rubert model from DeepPavlov",
            "datetime_created": datetime.now(),
            "by_unit": "text",
            "algorithm": "BERT",
            "pooling": "Average+Max",
            "meta_parameters": {
                "pre_trained": "rubert_cased_L-12_H-768_A-12_v1",
            },
        }
    )

    text_embedding_operators = []
    concurrency = 32
    for i in range(concurrency):
        text_embedding_operators.append(DjangoOperator(
            task_id=f"gen_text_average_max_embedding_{i}",
            python_callable=pool_embeddings,
            op_kwargs={"start": (100 / concurrency) * i,
                       "end": (100 / concurrency) * (i + 1),
                       "corpus": "main",
                       "from_embedding_name": SENTENCE_EMBEDDING_NAME,
                       "from_embedding_by_unit": "sentence",
                       "to_embedding_name": TEXT_EMBEDDING_NAME,
                       "to_embedding_by_unit": "text",
                       "pooling": "Average+Max",
                       }
        ))

    persist_text_embeddings = DjangoOperator(
        task_id="persist_text_average_max_embeddings",
        python_callable=persist_embeddings,
        op_kwargs={
            "corpus": "main",
            "embedding_name": TEXT_EMBEDDING_NAME,
            "by_unit": "text",
            "type_unit_int": 5,
            "algorithm": "bert",
            "pooling": "Average+Max",
            "description": "Bert text average+max pooling embedding using Rubert model from DeepPavlov"
        }
    )

    sentence_embedding_operators >> init_text_index >> text_embedding_operators >> persist_text_embeddings
