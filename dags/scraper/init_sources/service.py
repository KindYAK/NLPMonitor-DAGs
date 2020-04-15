from util.util import transliterate_for_dag_id


def init_sources(**kwargs):
    from airflow.models import Variable
    from mainapp.models import Source
    import json

    ss = Source.objects.filter(corpus__name__in=["main", "rus", "rus_propaganda"]).exclude(scraprules=None)
    Variable.set("sources",
                     json.dumps(
                         [
                             {
                                 "id": s.id,
                                 "name": transliterate_for_dag_id(s.name),
                                 "url": s.url
                             } for s in ss
                         ]
                     )
                 )
