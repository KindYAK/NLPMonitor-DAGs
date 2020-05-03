from airflow import DAG
from datetime import datetime, timedelta
from DjangoOperator import DjangoOperator
# from airflow.operators.python_operator import PythonVirtualenvOperator
# from PythonVirtualenvCachedOperator import PythonVirtualenvCachedOperator


def clean_text(s):
    from gensim import utils
    import gensim.parsing.preprocessing as gsp

    def replaces_special_chars(s):
        return s.replace('_', '').replace('\ufeff', '')

    filters = [
        gsp.strip_tags,
        gsp.strip_multiple_whitespaces,
        gsp.strip_short,
        replaces_special_chars
    ]
    s = s.lower()
    s = utils.to_unicode(s)
    for f in filters:
        s = f(s)
    return s


def test_connections_to_bert_service(created):
    print(f'starting task at {created}')
    from bert_serving.client import BertClient
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
    from elasticsearch_dsl import Search

    bc = BertClient(ip="bert_as_service")

    ind_doc_search = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    ind_doc_search = ind_doc_search.source(['id', 'text'])
    ind_doc_scan = ind_doc_search.scan()

    elastic_results = []

    for ind, res in enumerate(ind_doc_scan):
        if ind % 100 == 0:
            break
        if ind % 25 == 0 and not ind == 0:
            vecs = bc.encode(
                [i['text'] for i in elastic_results]
            ).tolist()
            for ind, vector in enumerate(vecs):
                elastic_results[ind].update({'rubert_embedding': vector})
            elastic_results = []

        elastic_results.append({'id': res.id, 'text': clean_text(res.text)})
        print(elastic_results)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 3),
    'email': ['bekbaganbetov.abay@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 20,
    'pool': 'long_tasks',
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
