def init_sources(**kwargs):
    from util.util import transliterate_for_dag_id

    from airflow.models import Variable
    from mainapp.models import Source
    import json

    ss = Source.objects.exclude(scraprules=None)
    Variable.set("sources",
                     json.dumps(
                         [
                             {
                                 "id": s.id,
                                 "name": transliterate_for_dag_id(s.name) if any([c in "ёйцукенгшщзхъфывапролджэячсмитьбю" for c in s.name.lower()]) else s.name,
                                 "url": s.url,
                                 "perform_full": s.id in kwargs['sources_full']
                             } for s in ss
                         ]
                     )
                 )
