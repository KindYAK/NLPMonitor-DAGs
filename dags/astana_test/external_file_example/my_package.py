def test():
    from mainapp.models import Corpus
    print("!!!", Corpus.objects.count())
    import xlrd
    print(xlrd.__version__)
    return xlrd.__version__
