import os
import datetime

from util.constants import BASE_DAG_DIR
from util.util import save_obj, load_obj


def generate_cooccurrence_codistance(**kwargs):
    import numpy as np
    from sklearn.feature_extraction.text import CountVectorizer
    from sklearn.metrics.pairwise import pairwise_distances
    from util.service_es import search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DICTIONARY_WORD

    max_dict_size = 30000
    if 'max_dict_size' in kwargs:
        max_dict_size = kwargs['max_dict_size']

    dictionary_words = search(ES_CLIENT, ES_INDEX_DICTIONARY_WORD,
                              query=kwargs['dictionary_filters'], source=("word_normal", ), sort=('_id', ),
                              get_search_obj=True)
    dictionary_words.aggs.bucket('unique_word_normals', 'terms', field='word_normal.keyword', size=max_dict_size)
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
    data_folder = os.path.join(BASE_DAG_DIR, "mussabayev_tm_temp")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    data_folder = os.path.join(data_folder, kwargs['name'])
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    save_obj(coocurance_matrix, os.path.join(data_folder, 'cooc_sparse_matrix.pkl'))

    print("!!!", "Start distance matrix calc", datetime.datetime.now())
    distance_matrix = pairwise_distances(coocurance_matrix, metric='cosine', n_jobs=4)
    print("!!!", "Save distance matrix ", datetime.datetime.now())
    save_obj(distance_matrix, os.path.join(data_folder, 'distance_matrix.pkl'))

    return f"Dictionary len={len(vectorizer.vocabulary_.keys())}, documents_len={documents_vectorized.shape[0]}"


def topic_modelling(**kwargs):
    import numpy as np
    import numba as nb
    from sklearn.metrics.pairwise import pairwise_distances
    from util.service_es import search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_DICTIONARY_WORD
    from .clustering_util import unique_clots, clots_binding, object_weighting1, cluster_weighting1, full_weighting1, \
        n_extra_objects, n_key_objects, save_clustering

    '''
    CLOT IN THE NEIGHBORHOOD
    Finding the clot in the neighborhood of the object defined by its pairwise distance matrix dm 
    The maximal intra-cluster distance is specified by the parameter d1
    None of the pairwise distances in formed clots exceeds d1
    start_ind - index of object which will be used as starting point for clot growing
    Returns object indices included to the builded clot
    ###################################
    СГУСТОК В ОКРЕСТНОСТИ
    Нахождение сгустка в окрестности объекта заданной dm - квадратной матрицей попарных расстояний
    от анализируемого объекта до всех объектов в его окрестности
    Максимальное внутрикластерное расстояние задаются параметром d1
    В формируемых сгустках расстояние между любыми двумя объектами не превышает d1
    start_ind - индекс объекта который будет использован как начальная точка роста при формировании сгустка
    Возвращает индексы объектов включённых в сгусток
    '''
    @nb.jit(nopython=True)
    def single_clot(dm, d1, start_ind):
        n = dm.shape[0]

        if start_ind < 0 or start_ind > n - 1:
            raise ValueError('start_ind is out of bounds')

        R = np.array([start_ind])
        C = np.delete(np.arange(n), start_ind)

        while len(C) > 0:
            C = C[np.sum(dm[R][:, C] <= d1, axis=0) == len(R)]

            if len(C) > 0:
                dist_sum = np.sum(dm[R][:, C], axis=0)

                best_ind = np.argsort(dist_sum)[0]

                R = np.append(R, C[best_ind])

                C = np.delete(C, best_ind)

        return R

    '''
    CLOTS FOR ALL OBJECTS
    Starting the process of finding clots for all objects in their neighbourhood
    defined by circle with center in considered object and radius d2
    Maximal pairwise intra-clot distance is specified by the parameter d1
    D - symmetric square matrix of pairwise distances between objects.
    Each clot is defined by indices of included objects
    use_medoid - if True then the medoids will be used as initial growing points, otherwise -
    the circle centers.
    Returns the array where each i-th element is clot in neighbourhood of i-th object.
    ###################################
    СГУСТКИ ДЛЯ КАЖДОГО ИЗ ОБЪЕКТОВ
    Запустает процесс выявления сгустков для каждого из объектов в их окрестности
    заданной окружностью с центорм в рассматриваемом объекте и радиусом d2
    Максимально допустимое попарное расстояние внутри сгустка задаётся параметром d1
    D - матрица попарных межобъектных расстояний между объектами
    Каждый сгусток задан множеством индексов включённых в него объектов
    use_medoid - если True, то в качестве точек роста сгустков будут использоваться медоиды. В противном случае -
    центры окружностей.
    Возвращает массив в котором каждый i-ый элемент является сгустком в окрестности i-го объекта.
    '''
    @nb.njit(parallel=True)
    def all_clots(D, d1, d2, use_medoid=True):
        n = D.shape[0]
        global_inds = np.arange(n)

        clots = [np.array([0])] * n

        for i in nb.prange(n):
            local_inds = global_inds[D[i] <= d2]

            if len(local_inds) > 0:

                dm = D[local_inds][:, local_inds]

                if use_medoid:
                    start_ind = np.argmin(np.sum(dm, axis=0))
                else:
                    start_ind = np.where(local_inds == i)[0][0]

                clot = single_clot(dm, d1, start_ind)

                if len(clot) > 0:
                    clots[i] = local_inds[clot]
                else:
                    clots[i] = np.empty(0, dtype=nb.int64)
            else:
                clots[i] = np.empty(0, dtype=nb.int64)

        return clots

    print("!!!", "Initial stuff", datetime.datetime.now())
    max_dict_size = 10000000
    if 'max_dict_size' in kwargs:
        max_dict_size = kwargs['max_dict_size']
    name = kwargs['name']
    d1 = kwargs['d1']
    d2 = kwargs['d2']
    d3 = kwargs['d3']
    min_clot_size = kwargs['min_clot_size']
    use_medoid = kwargs['use_medoid']

    dictionary_words = search(ES_CLIENT, ES_INDEX_DICTIONARY_WORD,
                              query=kwargs['dictionary_filters'], source=("word_normal", ), sort=('_id', ),
                              get_search_obj=True, end=max_dict_size)
    dictionary_words.aggs.bucket('unique_word_normals', 'terms', field='word_normal.keyword')
    vocab = [dw.key for dw in dictionary_words.execute().aggregations.unique_word_normals.buckets]

    data_folder = os.path.join(BASE_DAG_DIR, "mussabayev_tm_temp", name)
    distance_matrix = np.array(load_obj(os.path.join(data_folder, 'distance_matrix.pkl')), dtype=np.float32)
    cooccurrence_matrix = load_obj(os.path.join(data_folder, 'cooc_sparse_matrix.pkl'))
    matrix_dimensions = distance_matrix.shape[0]

    print("!!!", "Start all_clots", datetime.datetime.now())
    # Запускаем процесс поиска сгустков по каждому из объектов
    # Увеличение d1 приводит к увеличению: окрестности поиска,количества и размеров получаемых сгустков,
    # времени вычисления и объёма использованной памяти
    a_clots = all_clots(distance_matrix, d1, d2, use_medoid)
    print('!!!', 'All clot count: ' + str(len(a_clots)))

    print("!!!", "Start unique_clots", datetime.datetime.now())
    # Оставляем только уникальные сгустки
    clots = unique_clots(a_clots, min_clot_size)
    save_obj(data_folder, 'clots.pkl')
    print('!!!', 'Count of unique clots: ' + str(len(clots)))

    print("!!!", "Start clots_binding", datetime.datetime.now())
    # Cвязывание сгустков в единые кластера по мере их пересечения
    clusters = clots_binding(clots, d3, -1)
    print('!!!', 'Cluster count: ' + str(len(clusters)))

    print("!!!", "Start object_weighting1", datetime.datetime.now())
    # Взвешивание объектов внутри каждого кластера
    # Метод 1 - на основе взаимных расстояний
    object_weights1 = object_weighting1(clusters, distance_matrix)
    # Для каждого кластера определяем заданное nk количество ключевых объектов
    # по максимуму полученных весовых коэффициентов
    nk = 3
    key_objects1 = n_key_objects(nk, clusters, object_weights1)

    # Вычисление коэффициентов соответствия кластеру для полного множества объектов по всему множеству кластеров
    # Т.е. для для всех объектов полного множества получаем коэффициенты соответствия каждому из
    # полученных кластеров
    # Для каждого кластера определяем заданное ne количество дополнительных объектов (кандидатов на включение в кластер)
    ne = 3
    # Метод 1 - на основе взаимных расстояний
    full_weights1 = full_weighting1(clusters, distance_matrix)

    # На основе полученных коэффициентов осуществляем выбор в количестве ne объектов
    # имеющих максимальные коэффициенты соответствия кластеру но не включённых в рассматриваемый кластер
    extra_objects1 = n_extra_objects(ne, clusters, full_weights1)

    print("!!!", "Start cluster_weighting1", datetime.datetime.now())
    # Взвешивание каждого из полученных кластеров по мере их соответствия полному исходному множеству объектов
    # Каждому кластеру присваивается весовой коэффициент отражающий насколько хорошо объекты входящие в кластер
    # согласуются с полным множеством всех объектов
    cluster_weights1 = cluster_weighting1(clusters, distance_matrix)  # Method 1

    save_clustering(clusters, cluster_weights1, object_weights1, key_objects1, extra_objects1, vocab,
                    os.path.join(data_folder, "result_example.txt"))

    return f"Dictionary len={matrix_dimensions}, documents_len={'???TODO'}"
