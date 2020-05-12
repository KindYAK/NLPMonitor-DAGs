import pickle

import numba as nb
import numpy as np

from util.service_es import search


def not_implemented():
    raise Exception("Not implemented")


def is_word(text, threshold=0.5):
    if text:
        word_ratio = sum([c in "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNMАаБбВвГгДдЕ"
                           "еЁёЖжЗзИиЙйКкЛлМмНнОоПпСсТтУуФфХхЦцЧчШшЩщЪъЫыЬьЭэЮюЯя" for c in text]) / len(text)
        return word_ratio > threshold
    else:
        return False


def is_kazakh(text, threshold=0.05):
    kazakh_ratio = sum([c in "ӘәҒғҚқҢңӨөҰұҮүІі" for c in text]) / len(text)
    return kazakh_ratio > threshold if text else False


def is_latin(text, threshold=0.51):
    latin_ratio = sum([c in "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM" for c in text]) / len(text)
    return latin_ratio > threshold if text else False


def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


def save_obj(obj, name):
    pickle.dump(obj, open(name + '.pkl', 'wb'), protocol=4)


def geometrical_mean(data):
    a = np.log(data)
    return np.exp(a.sum() / len(a))


def parse_topics_field(tm):
    tm_dict = {}
    tm_name = tm.name
    for i, topic in enumerate(tm.topics):
        tm_dict[f'topic_{i}'] = [word['word'] for word in topic.topic_words]

    return tm_dict, tm_name


def get_clean_tms(full_corpus):
    tm_new_1 = {}
    tm_new_2 = {}
    equator = int(len(full_corpus) / 2)
    for i, elem in enumerate(full_corpus):
        if i < equator:
            tm_new_1[f'topic_{i}'] = elem.split()
        else:
            tm_new_2[f'topic_{i - equator}'] = elem.split()

    tm_new_1 = get_not_empty_tms(tm_new_1)
    tm_new_2 = get_not_empty_tms(tm_new_2)

    return tm_new_1, tm_new_2


def get_not_empty_tms(tm):
    out_tm = {}
    for key, val in tm.items():
        if len(val) >= 1:
            out_tm[key] = val
    return out_tm


def semantic_jaccard_util(tm):
    """
    pass
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    tfidf_vec = TfidfVectorizer()
    corpus = [" ".join(topic) for topic in tm.values()]
    vectorized_data = tfidf_vec.fit_transform(raw_documents=corpus)

    index_value = {i[1]: i[0] for i in tfidf_vec.vocabulary_.items()}

    fully_indexed = []
    for row in vectorized_data:
        fully_indexed.append({index_value[column]: value for (column, value) in zip(row.indices, row.data)})

    w_tfidf = weights_tfidf(list(tm.values()), fully_indexed)
    w_idf = []
    for w in w_tfidf:
        w_idf.append(w / max(w))
    return w_idf


def weights_tfidf(doci, indexed):
    w_tfidf = []
    for i in range(len(doci)):
        doc = doci[i]
        w = np.zeros(len(doc))
        for j in range(len(doc)):
            w[j] = indexed[i].get(doc[j])
        w_tfidf.append(w)

    return w_tfidf


@nb.njit(parallel=True, nopython=True)
def mutual_cross_similarity(similarity_matrix, row_weights, col_weights, similarity_threshold):
    n_rows = len(row_weights)
    n_cols = len(col_weights)
    assert (similarity_matrix.shape == (n_rows, n_cols))
    complete_connection_value = 0  # numer
    true_connection_value = 0  # denom

    for i in nb.prange(n_rows):
        for j in range(n_cols):
            complete_connection_value += row_weights[i] * col_weights[j] * similarity_matrix[i, j]
            if similarity_matrix[i, j] >= similarity_threshold:
                true_connection_value += row_weights[i] * col_weights[j] * similarity_matrix[i, j]

    if complete_connection_value > 0:
        connection_value = true_connection_value / complete_connection_value
    else:
        connection_value = 0

    return connection_value


def sim(text1, text2, w_d, w_t, D, eps):
    sim = np.zeros((len(text1), len(text2)))
    for d in range(len(text1)):
        for t in range(len(text2)):
            sim[d, t] = 1 - D[text1[d], text2[t]]
    corr_matr = mutual_cross_similarity(sim, w_d, w_t, eps)

    return corr_matr


def cross_similarity_w2v(similarity_matrix, row_weights, col_weights, similarity_threshold, p, neg1, neg2, idx1, idx2):
    n_rows = len(row_weights)
    n_cols = len(col_weights)
    assert (similarity_matrix.shape == (n_rows, n_cols))
    complete_connection_value = 0
    true_connection_value = 0

    for i in nb.prange(n_rows):
        complete_connection_value += (row_weights[i] * col_weights[idx1[i]]) * (similarity_matrix[i, idx1[i]]) * neg1[i]
        for j in range(n_cols):
            if i == 0:
                complete_connection_value += (row_weights[idx2[j]] * col_weights[j]) * (similarity_matrix[idx2[j], j]) * \
                                             neg2[j]

            if similarity_matrix[i, j] >= similarity_threshold:
                true_connection_value += (row_weights[i] * col_weights[j]) * (p + similarity_matrix[i, j])

    if complete_connection_value > 0:
        connection_value = true_connection_value / complete_connection_value
    else:
        connection_value = 0

    return connection_value


def sim_w2v(text1, text2, w_d, w_t, D, eps, p=-0.8):
    M = np.zeros((len(text1), len(text2)))

    for d in range(len(text1)):
        for t in range(len(text2)):
            M[d, t] = 1 - D[text1[d], text2[t]]

    max1 = np.array([np.amax(M[m, :]) for m in range(M.shape[0])])
    # max value from each word (index) in sent1 to words (values in list) in sent2
    indmax1 = [np.where(M[m, :] == max1[m])[0][0] for m in range(M.shape[0])]
    max2 = np.array([np.amax(M[:, n]) for n in range(M.shape[1])])
    indmax2 = [np.where(M[:, n] == max2[n])[0][0] for n in range(M.shape[1])]

    # find which max similarities are out of one sigma
    find1 = (max1 - np.mean(max1) + np.std(max1))
    find2 = (max2 - np.mean(max2) + np.std(max2))

    # maximum similarity of words in sent1 is significant in regards to words in sent2
    neg1 = (find1 > 0) * 1
    neg2 = (find2 > 0) * 1

    sim_output = cross_similarity_w2v(M, w_d, w_t, eps, p, neg1, neg2, indmax1, indmax2)
    return sim_output


def commindex(voc, doc):
    cluster = np.zeros(len(doc), dtype=np.int64)
    for d in range(len(doc)):
        cluster[d] = voc[doc[d]]
    return cluster


def corp2ind(corpus, voc):
    indCorp = [np.array(commindex(voc, d)) for d in corpus]
    return indCorp


def mapper(topic_seq_1, topic_seq_2, threshold_list):
    """
    pass
    """
    from sklearn.metrics import pairwise_distances
    from sklearn.feature_extraction.text import CountVectorizer

    mapping_dict, delta_words_dict, delta_count_dict = dict(), dict(), dict()

    full_corpus = [' '.join(topic) for tm in [topic_seq_1, topic_seq_2] for topic in tm.values()]

    full_corpus = [elem.replace('-', '') if '-' in elem else elem for elem in full_corpus]

    topic_seq_1_cleaned, topic_seq_2_cleaned = get_clean_tms(full_corpus)

    c_vect = CountVectorizer()
    vect_corpus_full = c_vect.fit_transform(full_corpus)
    cooc = vect_corpus_full.T.dot(vect_corpus_full).astype(np.uint32)

    D = pairwise_distances(cooc, metric='cosine', n_jobs=-1)

    vocab = c_vect.get_feature_names()

    Vocab = {}
    for index in range(len(vocab)):
        Vocab[vocab[index]] = index

    corp2ind_data_topics_1 = corp2ind(list(topic_seq_1_cleaned.values()), Vocab)
    corp2ind_data_topics_2 = corp2ind(list(topic_seq_2_cleaned.values()), Vocab)

    w_idf_topics_1 = semantic_jaccard_util(topic_seq_1_cleaned)
    w_idf_topics_2 = semantic_jaccard_util(topic_seq_2_cleaned)

    for threshold in threshold_list:
        mapping, delta_words, delta_count = semantic_jaccard_mapper(topics_dict_1=topic_seq_1_cleaned,
                                                                    topics_dict_2=topic_seq_2_cleaned,
                                                                    threshhold=threshold,
                                                                    corp2ind_data_topics_1=corp2ind_data_topics_1,
                                                                    corp2ind_data_topics_2=corp2ind_data_topics_2,
                                                                    D=D,
                                                                    w_idf_topics_1=w_idf_topics_1,
                                                                    w_idf_topics_2=w_idf_topics_2)
        mapping_dict[threshold] = mapping
        delta_words_dict[threshold] = delta_words
        delta_count_dict[threshold] = delta_count

    return mapping_dict, delta_words_dict, delta_count_dict


def semantic_jaccard_mapper(topics_dict_1, topics_dict_2, threshhold, corp2ind_data_topics_1, corp2ind_data_topics_2,
                            w_idf_topics_1, w_idf_topics_2, D):
    """
    pass
    """
    topics_mapping_dict = {}
    topics_delta_words = {}
    topics_delta_counts = {}
    for i, key in enumerate(topics_dict_1.keys()):
        for j, key_1 in enumerate(topics_dict_2.keys()):
            if sim_w2v(text1=corp2ind_data_topics_1[i],
                       text2=corp2ind_data_topics_2[j],
                       w_d=w_idf_topics_1[i],
                       w_t=w_idf_topics_2[j],
                       D=D,
                       eps=0.97) > float(threshhold):  # fixed eps=0.6
                diff_words = list(set(topics_dict_1[key]).difference(topics_dict_2[key_1]))
                if key in topics_mapping_dict.keys():
                    topics_mapping_dict[key] += [key_1]
                    topics_delta_words[key] += [{key_1: diff_words}]
                    topics_delta_counts[key] += [{key_1: len(diff_words)}]
                else:
                    topics_mapping_dict[key] = [key_1]
                    topics_delta_words[key] = [{key_1: diff_words}]
                    topics_delta_counts[key] = [{key_1: len(diff_words)}]
    return topics_mapping_dict, topics_delta_words, topics_delta_counts


def validator(mappings_dict, client, index_theta_one, index_theta_two, datetime_from_tm_2, datetime_to_tm_1,
              number_of_topics):
    """
    pass
    """
    from sklearn.preprocessing import MinMaxScaler
    from nltk.metrics import jaccard_distance
    scaler = MinMaxScaler()
    scores = dict(zip(mappings_dict.keys(), [0] * len(mappings_dict)))
    scores_for_normalization = []
    for threshhold, map_dict in mappings_dict.items():
        cnt_matches_for_threshhold = 0
        for topic_parent, topic_childs_list in map_dict.items():

            theta_1 = search(client=client, index=index_theta_one,
                             query={'datetime__gte': datetime_from_tm_2, 'datetime__lte': datetime_to_tm_1,
                                    'topic_id': topic_parent, 'topic_weight__gte': 0.05},
                             source=['document_es_id'],
                             start=0,
                             end=1000000,
                             get_scan_obj=True
                             )
            scanned_parent = set([elem.document_es_id for elem in theta_1])

            for topic_child in topic_childs_list:
                theta_2 = search(client=client, index=index_theta_two,
                                 query={'datetime__gte': datetime_from_tm_2, 'datetime__lte': datetime_to_tm_1,
                                        'topic_id': topic_child, 'topic_weight__gte': 0.05},
                                 source=['document_es_id'],
                                 start=0,
                                 end=1000000,
                                 get_scan_obj=True
                                 )
                jaccard_score = 1 - jaccard_distance(scanned_parent, set([elem.document_es_id for elem in theta_2]))

                scores[threshhold] += jaccard_score
                cnt_matches_for_threshhold += 1
        try:
            avg_score = scores[threshhold] / cnt_matches_for_threshhold

            scores_for_normalization.append(avg_score)
            scores[threshhold] = [len(map_dict) / number_of_topics, avg_score]

        except ZeroDivisionError:
            scores[threshhold] = [len(map_dict) / number_of_topics, 0]

    scores_normalized = [score[0] for score in scaler.fit_transform(np.array(scores_for_normalization).reshape(-1, 1))]

    for i, items in enumerate(scores.items()):
        scores[items[0]] += [scores_normalized[i]]

    return scores


def shards_mapping(doc_count: int) -> int:
    if isinstance(doc_count, str):
        doc_count = int(doc_count)

    if doc_count > 10_000_000:
        return 5
    elif doc_count > 1_000_000:
        return 3
    elif doc_count > 100_000:
        return 2
    else:
        return 1


def jaccard_similarity(list1, list2):
    if not list1 or not list2:
        return 0
    intersection = len(set(list1).intersection(list2))
    union = (len(list1) + len(list2)) - intersection
    return intersection / union


def transliterate_for_dag_id(name):
    from transliterate import translit
    name_translit = translit(name, 'ru', reversed=True)
    return clear_symbols(name_translit)


def clear_symbols(name):
    return "".join([c for c in name.replace(" ", "_") if (c.isalnum() or c in ["_", ".", "-"]) and c not in "әғқңөұүі"]).strip().lower()