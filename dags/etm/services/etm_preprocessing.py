def preprocess_data(**kwargs):
    """

    :param kwargs:
    :return:
    """
    import os
    import pickle

    import numpy as np

    from scipy.io import savemat
    from sklearn.feature_extraction.text import CountVectorizer

    from util.service_es import search
    from util.constants import BASE_DAG_DIR
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    from .utils import split_bow, remove_empty, create_bow, create_doc_indices, create_list_words

    corpus = kwargs.get('corpus', 'main')
    test_size = kwargs.get('test_size', 0.1)

    max_df = 0.7
    min_df = 100  # choose desired value for min_df

    # Read data
    print('reading text file...')
    docs = search(client=ES_CLIENT, index=ES_INDEX_DOCUMENT, start=0, end=1_000_000, query={'corpus': corpus},
                  source=['text_lemmatized'], get_scan_obj=True)
    docs = [' '.join(doc) for doc in docs.text_lemmatized]
    #  Create count vectorizer
    print('counting document frequency of words...')
    cvectorizer = CountVectorizer(min_df=min_df, max_df=max_df, stop_words=None)
    cvz = cvectorizer.fit_transform(docs).sign()

    #  Get vocabulary
    print('building the vocabulary...')
    sum_counts = cvz.sum(axis=0)
    v_size = sum_counts.shape[1]
    sum_counts_np = np.zeros(v_size, dtype=int)
    for v in range(v_size):
        sum_counts_np[v] = sum_counts[0, v]
    word2id = dict([(w, cvectorizer.vocabulary_.get(w)) for w in cvectorizer.vocabulary_])
    del cvectorizer
    print('  initial vocabulary size: {}'.format(v_size))

    #  Split in train/test/valid
    print('tokenizing documents and splitting into train/test/valid...')
    num_docs = cvz.shape[0]

    #  Remove words not in train_data
    vocab = [word for word in word2id.keys()]
    print('  vocabulary after removing words not in train: {}'.format(len(vocab)))

    docs_tr = [[word2id[w] for w in docs[idx_d].split() if w in word2id] for idx_d in range(num_docs)]
    docs_ts = docs_tr[:test_size]

    del docs
    print('  number of documents (train): {} [this should be equal to {}]'.format(len(docs_tr), num_docs))

    # Getting lists of words and doc_indices
    print('creating lists of words...')

    words_tr = create_list_words(docs_tr)
    words_ts = create_list_words(docs_ts)

    # Get doc indices
    print('getting doc indices...')

    doc_indices_tr = create_doc_indices(docs_tr)
    doc_indices_ts = create_doc_indices(docs_ts)

    #  Remove empty documents
    print('removing empty documents...')

    docs_tr = remove_empty(docs_tr)
    docs_ts = remove_empty(docs_ts)

    # Number of documents in each set
    n_docs_tr = len(docs_tr)
    n_docs_ts = len(docs_ts)

    # Create bow representation
    print('creating bow representation...')

    bow_tr = create_bow(doc_indices_tr, words_tr, n_docs_tr, len(vocab))
    bow_ts = create_bow(doc_indices_ts, words_ts, n_docs_ts, len(vocab))

    # Save vocabulary to file
    path_save = os.path.join(BASE_DAG_DIR, 'etm_temp')
    if not os.path.isdir(path_save):
        os.system('mkdir -p ' + path_save)

    with open(os.path.join(path_save, 'vocab.pkl'), 'wb') as f:
        pickle.dump(vocab, f)

    # Split bow intro token/value pairs
    print('splitting bow intro token/value pairs and saving to disk...')

    bow_tr_tokens, bow_tr_counts = split_bow(bow_tr, n_docs_tr)
    savemat(os.path.join(path_save, 'bow_tr_tokens.mat'), {'tokens': bow_tr_tokens}, do_compression=True)
    savemat(os.path.join(path_save, 'bow_tr_counts.mat'), {'counts': bow_tr_counts}, do_compression=True)

    bow_ts_tokens, bow_ts_counts = split_bow(bow_ts, n_docs_ts)
    savemat(os.path.join(path_save, 'bow_ts_tokens.mat'), {'tokens': bow_ts_tokens}, do_compression=True)
    savemat(os.path.join(path_save, 'bow_ts_counts.mat'), {'counts': bow_ts_counts}, do_compression=True)

    print('Data ready !!')
