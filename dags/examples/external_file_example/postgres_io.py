def postgres_etl(**kwargs):
    import datetime
    import random
    from mainapp.models import Document, Corpus, Source

    stuff = kwargs['stuff']

    # Extract
    documents = Document.objects.filter(source__corpus__name="main", num_views__lte=5, datetime__gte=datetime.date(2000, 1, 1)).exclude(num_views=None)
    print("!!!", documents.count())

    # Transform
    corp = Corpus.objects.create(name="Delete Later " + str(random.randint(0, 10000000)))
    source = Source.objects.create(name=f"New source - {stuff}", corpus=corp)
    for document in documents:
        document.num_views += 1
        document.source = source

    # Load
    Document.objects.bulk_update(documents, ['num_views', 'source'])
