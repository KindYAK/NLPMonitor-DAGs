"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonVirtualenvOperator, PythonOperator
from DjangoOperator import DjangoOperator
from datetime import datetime, date, timedelta

from dags.mussabayev_tm.service.service import generate_cooccurrence_codistance, topic_modelling


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'priority_weight': 55,
    'pool': 'long_tasks'
}

dag = DAG('Nlpmonitor_Mussabayev_tm', default_args=default_args, schedule_interval=None)


with dag:
    dictionary_filters = {
                "dictionary": "default_dict_pymorphy_2_4_393442_3710985",
                "document_normal_frequency__gte": 100,
                "document_normal_frequency__lte": 500000,
                "is_stop_word": False,
                # "is_in_pymorphy2_dict": True,
                # "is_multiple_normals_in_pymorphy2": False,
            }
    max_dict_size = 30000
    generate_cooccurrence_codistance = DjangoOperator(
        task_id="generate_cooccurrence_codistance",
        python_callable=generate_cooccurrence_codistance,
        op_kwargs={
            "name": "test",
            "dictionary_filters": dictionary_filters,
            "max_dict_size": max_dict_size,
            "document_filters": {
                "corpus": "main",
                # "source": "https://kapital.kz/",
                "datetime__gte": date(1950, 1, 1),
                "datetime__lte": date(2050, 1, 1),
            },
        }
    )

    topic_modelling_operator = DjangoOperator(
        task_id="topic_modelling",
        python_callable=topic_modelling,
        op_kwargs={
            "name": "test",
            "d1": 1.75, # Максимальное допустимое расстояние между всеми возможными попарными комбинациями объектов в составе формируемых сгустков (нечётких протокластеров)
            "d2": 1.75, # Радиус окрестности в пределах которой осуществляется поиск сгустков
            "d3": 0.0, # Минимально допустимая доля общности составов двух произвольных сгустков для установления связи между ними
            "min_clot_size": 2, # Минимально допустимое размер сгустка (количество объектов в сгустке). Сгустки меньшего размера удаляются
            "use_medoid": True, # Если True, то в качестве точек роста сгустков будут использоваться медоиды. В противном случае - центры окружностей
            "dictionary_filters": dictionary_filters,
            "max_dict_size": max_dict_size,
        }
    )

    generate_cooccurrence_codistance >> topic_modelling_operator
