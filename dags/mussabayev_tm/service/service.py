import os
import datetime

from util.util import save_obj, load_obj


def generate_coocurance_codistance(**kwargs):
    import numpy as np
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.metrics.pairwise import pairwise_distances
    from util.service_es import search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_WORD

    dictionary_words = search(ES_CLIENT, ES_INDEX_DICTIONARY_WORD,
                              query=kwargs['dictionary_filters'], source=("word_normal", ),
                              get_search_obj=True, end=10000000)
    dictionary_words.aggs.bucket('unique_word_normals', 'terms', field='word_normal.keyword')
    dictionary_words = dictionary_words.execute()
    documents_scan = search(ES_CLIENT, ES_INDEX_DOCUMENT,
                            query=kwargs['document_filters'], source=("text_lemmatized", ),
                            get_scan_obj=True, end=5000000)

    print("!!!", "Start count_vectorizing", datetime.datetime.now())
    vectorizer = CountVectorizer(vocabulary=(dw.key for dw in dictionary_words.aggregations.unique_word_normals.buckets))
    documents_vectorized = vectorizer.fit_transform((d.text_lemmatized for d in documents_scan))

    print("!!!", "Start dot product for coocurance matrix", datetime.datetime.now())
    coocurance_matrix = documents_vectorized.T.dot(documents_vectorized).astype(np.uint32)
    print("!!!", "Saving coocurance matrix", datetime.datetime.now())
    save_obj(coocurance_matrix, 'cooc_sparse_matrix')

    print("!!!", "Start distance matrix calc", datetime.datetime.now())
    distance_matrix = pairwise_distances(coocurance_matrix, metric='cosine', n_jobs=4)
    print("!!!", "Save distance matrix ", datetime.datetime.now())
    save_obj(distance_matrix, 'distance_matrix')

    return f"Dictionary len={len(vectorizer.vocabulary_.keys())}, documents_len={documents_vectorized.shape[0]}"
