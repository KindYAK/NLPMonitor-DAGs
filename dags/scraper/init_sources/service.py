def init_sources(**kwargs):
    from airflow.models import Variable
    from mainapp.models import Source
    import json

    ss = Source.objects.filter(corpus="main")
    Variable.set("sources",
                     json.dumps(
                         [{
                             "id": s.id,
                             "name": s.name,
                             "url": s.url
                         } for s in ss]
                     )
                 )


# def test(**kwargs):
#     import json
#     from airflow.models import Variable
#
#     ss = json.loads(Variable.get('sources'))
#     print(ss)
#
#     return len(ss)
