def test():
    from nlpmonitor.settings import ES_CLIENT
    from mainapp.models import Corpus

    from elasticsearch_dsl import Search, ES_INDEX_DOCUMENT

    print("!!! Checking DB access", Corpus.objects.count())
    print("!!! Checking ElasticSearch access", Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).count())
    import xlrd
    print("!!! Checking venv access", xlrd.__version__)
    return "OK"
