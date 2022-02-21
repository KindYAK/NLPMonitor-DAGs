from datetime import datetime, timedelta

from dags.bigartm.fill_dags.fill_dags_2019 import fill_dags_2019
from dags.bigartm.fill_dags.fill_dags_2020 import fill_dags_2020
from dags.bigartm.fill_dags.fill_dags_full import fill_dags_full
from dags.bigartm.fill_dags.fill_dags_healthcare import fill_dags_healthcare
from dags.bigartm.fill_dags.fill_dags_healthcare_2022 import fill_dags_healthcare_2022
from dags.bigartm.fill_dags.fill_dags_news_and_gos import fill_dags_news_and_gos
from dags.bigartm.fill_dags.fill_dags_ngramized import fill_dags_ngramized
from dags.bigartm.fill_dags.fill_dags_rus_corpora import fill_dags_rus_corpora
from dags.bigartm.fill_dags.fill_dags_scientometrics import fill_dags_scientometrics
from dags.bigartm.fill_dags.fill_dags_scopus import fill_dags_scopus
from dags.bigartm.fill_dags.fill_dags_small import fill_dags_small
from dags.bigartm.fill_dags.fill_dags_two_years import fill_dags_two_years
from dags.bigartm.fill_dags.fill_dags_kz import fill_dags_kz

# DAG Config is in fill_dag/utils.py
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

actualizable_bigartms = []
comboable_bigartms = []

dag_full = fill_dags_full(actualizable_bigartms, comboable_bigartms)
dag_two_years = fill_dags_two_years(actualizable_bigartms, comboable_bigartms)
dag_2019 = fill_dags_2019(actualizable_bigartms, comboable_bigartms)
dag_2020 = fill_dags_2020(actualizable_bigartms, comboable_bigartms)
dag_small = fill_dags_small(actualizable_bigartms, comboable_bigartms)
dag_news_and_gos = fill_dags_news_and_gos(actualizable_bigartms, comboable_bigartms)
dag_scientometrics = fill_dags_scientometrics(actualizable_bigartms, comboable_bigartms)
dag_scopus = fill_dags_scopus(actualizable_bigartms, comboable_bigartms)
dag_ngramized = fill_dags_ngramized(actualizable_bigartms, comboable_bigartms)
dag_rus_corpora = fill_dags_rus_corpora(actualizable_bigartms, comboable_bigartms)
dag_kz = fill_dags_kz(actualizable_bigartms, comboable_bigartms)
dag_healthcare = fill_dags_healthcare(actualizable_bigartms, comboable_bigartms)
dag_healthcare_2022 = fill_dags_healthcare_2022(actualizable_bigartms, comboable_bigartms)
