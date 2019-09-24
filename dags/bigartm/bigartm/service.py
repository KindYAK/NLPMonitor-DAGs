from dags.bigartm.bigartm.cleaners import get_wordnet_pos, lemmatizer_func, stop_words_remover, \
                                          return_cleaned_array, txt_writer


def dataset_prepare():
    # import pandas as pd
    import sklearn
    import nltk
    import re
    import numpy as np

    from util.service_es import search, update_generator, get_count
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    # Extract
    query = {
        "corpus": "main",

    }
    num_documents = get_count(ES_CLIENT, ES_INDEX_DOCUMENT)
    print(num_documents)
    # documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query, end=num_documents)



def topic_modelling():
    import artm
    import pandas as pd
    import nltk
    import re
    import numpy as np
