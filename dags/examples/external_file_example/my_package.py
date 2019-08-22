def some_complicated_stuff():
    s = "Prize sektor on BARABAN, LETTER!!!"
    letter = "B"
    import xlrd
    print(xlrd.__version__)
    return "NO SUCH LETTER, KEEP ROLLING THE BARABAN!!".encode("utf-8")


def test():
    from mainapp.models import Corpus
    import random
    Corpus.objects.create(name="Delte Later" + str(random.randint(0, 10000000)))
    import xlrd
    print(xlrd.__version__)
    return xlrd.__version__
