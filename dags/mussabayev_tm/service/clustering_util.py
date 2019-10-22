def save_clustering(clusters, cluster_weights, object_weights, key_objects, extra_objects, vocab, filename):
    f = open(filename, "w")
    for i in range(len(clusters)):
        f.write('Кластер №' + str(i) + ' ' + str(round(cluster_weights[i], 2)) + '\n')

        s = ''
        for j in range(len(key_objects[i][0])):
            key_object = key_objects[i][0][j]
            object_weight = key_objects[i][1][j]
            if s == '':
                s = vocab[key_object].upper() + ' ' + str(round(object_weight, 2))
            else:
                s += ', ' + vocab[key_object].upper() + ' ' + str(round(object_weight, 2))
        f.write('<< ' + s + ' >>\n')

        b = True
        for j in range(len(clusters[i])):
            s = vocab[clusters[i][j]] + ' ' + str(round(object_weights[i][j], 2))
            if b:
                f.write(s)
                b = False
            else:
                f.write(', ' + s)
        f.write('\n')

        f.write('*********************************************************** \n')
        s = ''
        for j in range(len(extra_objects[i][0])):
            extra_object = extra_objects[i][0][j]
            object_weight = extra_objects[i][1][j]
            if s == '':
                s = vocab[extra_object] + ' ' + str(round(object_weight, 2))
            else:
                s += ', ' + vocab[extra_object] + ' ' + str(round(object_weight, 2))
        f.write('Кандидаты: ' + s + ' \n')

        f.write('\n')
    f.close()


# UNIQUE CLOTS
# Finning unique clots
# In the result included the clots which sizes (count of included objects) not les than min_size
# Returns the list of unique clots represented as list() of set(),
# where set() - the set of indices making up each clot
# The same object can be included in several clots.
####################################
# УНИКАЛЬНЫЕ СГУСТКИ
# Ищем уникальные сгустки
# В результат включаются только сгустки размер которых (количество включённых объектов) не меньше min_size
# Возвращает набор уникальных сгустков представленных в виде list() of set(),
# где set() - множество индексов составляющих каждый сгусток.
# Один и тот же объект может быть включён в несколько сгустков
def unique_clots(clots, min_size=1):
    import numpy as np
    R = set()
    for clot in clots:
        if (len(clot) > 0) and (len(clot) >= min_size):
            R.add(frozenset(clot))
    return np.array([set(v) for v in R])


# BINDING CLOTS IN ONE CLUSTERS BY MEASURE OF THEIR INTERSECTIONS
# clots - an array of clots in which the i-th element is the set of indices of the objects making up this clot
# n_jobs - the number of parallel handlers that will be used to calculate the distance matrix
# between the clots. If n_jobs = -1, then all available number of handlers will be used
# (see n_jobs in sklearn.metrics.pairwise_distances)
# All clots are binding simultaneously in a cluster if they are by the modified Jaccard measure
# have a intersettion fraction at least 1-d3. Clots can be connected either directly or indirectly.
# In the case of a direct connection, the clots directly have a sufficient fraction of the intersection.
# An indirect connection between two clots is established if there is a chain of directly connected
# clots between them (i.e., they are indirectly linked through a chain of directly connected clots)
# Thus, all sets of directly or indirectly binded clots making up higher-level united clusters.
# Clots can be considered as a first level fuzzy clusters, which are by the measure of their intersections
# join together into higher-level clusters with a relatively more complex spatial structure.
# The idea of this clustering method is to split the entire set of objects into clots - simple mutually
# intersecting subsets of objects with following intercoupling them to the high-level clusters,
# which makes it possible to approximate complex spatial cluster structures by
# simple mutually intersecting clusters.
# As a result, the function returns an array, each element of which is a cluster consisting of
# indices of objects included in it
####################################
# СВЯЗЫВАНИЕ СГУСТКОВ В ЕДИНЫЕ КЛАСТЕРА ПО МЕРЕ ИХ ПЕРЕСЕЧЕНИЯ
# clots - массив сгустков в котором i-ый элемент является множеством индексов объектов составляющих данный сгусток
# n_jobs - количество параллельных обработчиков которые будут использованы для расчёта матрицы расстояний
# между сгустками. Если n_jobs = -1, то будет задайствовано всё возможное количество обработчиков.
# Единомоментно в кластер связываются все сгустки если они по модифицированной мере Жаккара
# имеют долю перечечение не менее 1-d3. Сгустки могут быть связаны как напрямую так и опосредованно.
# В случае прямой связи сгустки непосредственно имеют достаточную долю пересечения.
# Опосредованная связь между двумя сгустками устанавливается если между ними имеется
# цепочка напрямую связанных сгустков (т.е. они опосредованно связываются через цепочку напрямую связанных сгустков)
# Таким образом все множества напрямую или опосредонанно связанных сгустков образуют единые кластера.
# Сгустки можно рассматривать как нечёткие кластера первого уровня, которые по мере их пересечения объединяются
# в более высокоуровневые кластера с относительно более сложной пространственной структурой.
# Смысл данного метода кластеризации заключается в разбиении всего множества объектов
# на сгустки - простые взаимно пересекающиеся подмножества объектов с последующим
# установлением связей между ними, что позволяет аппроксимировать
# простыми взаимно пересекающимися сгустками кластеры имеющие сложные пространственные структуры.
# В результате функция возвращает массив каждый элемент которого является кластером состоящим из
# индексов включённых в него объектов
def clots_binding(clots, d3, n_jobs=-1):
    import numpy as np
    from sklearn.metrics.pairwise import pairwise_distances
    # a, b - индексы сгустков между которыми вычисляется расстояние по модифицированной мере Жаккара
    def jaccard_dist(a, b):
        return 1 - len(clots[np.int(a[0])].intersection(clots[np.int(b[0])])) / min(len(clots[np.int(a[0])]),
                                                                                    len(clots[np.int(b[0])]))

    m = len(clots)
    inds = np.reshape(np.arange(len(clots), dtype=np.int), (-1, 1))
    dm = pairwise_distances(inds, metric=jaccard_dist, n_jobs=n_jobs)
    labels = np.arange(m, dtype=np.int)
    inds = np.arange(m, dtype=np.int)
    for i in range(m):
        old_labels = labels[dm[i] < d3]
        for j in old_labels:
            np.place(labels, labels == j, labels[i])
    R = np.array([np.array(list(set().union(*clots[labels == i]))) for i in np.unique(labels)])
    return R


# Weighing of objects in the clusters considering their pairwise distances D (Method 1)
# An object acquires the greatest weight if its total pairwice distance to other objects in the cluster is minimal
####################################
# Взвешивание объектов в кластере (Метод 1)
# Объект получает тем больший вес чем меньше его суммарное расстояние до остальных объектов в кластере
def object_weighting1(clusters, D):
    import numpy as np
    def weights(cluster, D):
        dm = np.array(D[cluster][:, cluster], dtype=np.float)
        sm = np.sum(dm, axis=1)
        maxV = np.max(sm)
        if maxV > 0:
            return 1 - sm / maxV
        else:
            return np.ones(sm.shape, dtype=np.float)

    return np.array([weights(c, D) for c in clusters])


# Weighing of objects in the clusters considering their pairwise frequences represented in cooc (Method 2)
# An object acquires the greatest weight if its total pairwice frequency with other
# objects in the cluster is maximal
####################################
# Взвешивание объектов в кластере (Метод 2)
# Объект получает тем больший вес чем больше его суммарная совместная частота с каждым из объектов в кластере
def object_weighting2(clusters, cooc):
    import numpy as np
    def weights(cluster, cooc):
        dm = np.array(cooc[cluster][:, cluster], dtype=np.float)
        sm = np.sum(dm, axis=1)
        maxV = np.max(sm)
        if maxV > 0:
            return sm / maxV
        else:
            return np.zeros(sm.shape, dtype=np.float)

    return np.array([weights(c, cooc) for c in clusters])


# Weighing of a full set of objects by the measure of their equivalence to the existing clusters (Method 1).
# An object acquires the greatest weight if its total pairwice distance to
# other objects in the cluster is minimal.
# Returns the matrix with NxM size, where N - number of clusters, M - number of objects and
# elements are corresponding matching factors (weight coefficients) between i-th cluster and j-th object.
####################################
# Взвешивание полного набора объектов по мере их соответствия имеющимся кластерам (Метод 1).
# Объект получает тем больший вес чем меньше его суммарное расстояние до объектов сопоставляемого кластера
# Возвращает матрицу размером NxM, где N - количество кластеров, M - количество объектов, элементы матрицы -
# коэффициенты соответствия (веса) между i-ым кластером и j-ым объектом.
def full_weighting1(clusters, D):
    import numpy as np
    def weights(cluster, D):
        dm = np.array(D[:][:, cluster], dtype=np.float)
        sm = np.sum(dm, axis=1)
        maxV = np.max(sm)
        if maxV > 0:
            return 1 - sm / maxV
        else:
            return np.ones(sm.shape, dtype=np.float)

    return np.array([weights(c, D) for c in clusters])


# Weighing of a full set of objects by the measure of their equivalence to the existing clusters (Method 2).
# An object acquires the greatest weight if its total pairwice frequency with
# other objects in the cluster is maximal.
# Returns the matrix with NxM size, where N - number of clusters, M - number of objects and
# elements are corresponding matching factors (weight coefficients) between i-th cluster and j-th object.
####################################
# Взвешивание полного набора объектов по мере их соответствия имеющимся кластерам (Метод 2).
# Объект получает тем больший вес чем больше его суммарная совместная частота
# с каждым из объектов в сопоставляемом кластере.
# Возвращает матрицу размером NxM, где N - количество кластеров, M - количество объектов, элементы матрицы -
# коэффициенты соответствия (веса) между i-ым кластером и j-ым объектом.
def full_weighting2(clusters, cooc):
    import numpy as np
    def weights(cluster, cooc):
        dm = np.array(cooc[:][:, cluster], dtype=np.float)
        sm = np.sum(dm, axis=1)
        maxV = np.max(sm)
        if maxV > 0:
            return sm / maxV
        else:
            return np.zeros(sm.shape, dtype=np.float)

    return np.array([weights(c, cooc) for c in clusters])


# Select the first n key objects for each of the clusters from the objects in considered cluster
# by their weights
####################################
# Выбирает n первых ключевых объектов для каждого из кластера из объектов входящих в кластер по их весам
def n_key_objects(n, clusters, weights):
    import numpy as np
    m = len(clusters)
    R = []
    for i in range(m):
        W = np.argsort(weights[i])[::-1][:n]
        R.append([clusters[i][W], weights[i][W]])
    return R


# Select the first n additional objects (candidates for inclusion to the cluster)
# for each of the cluster from among all objects not included to it.
# Selection is performed by corresponding weights.
####################################
# Выбирает n первых дополнительных объектов (кандидатов для включения в кластер)
# для каждого из кластера из числа всех объектов не входящих в рассматриваемый кластер.
# Выбор осуществляется по соответствующим весам (weights).
def n_extra_objects(n, clusters, weights):
    import numpy as np
    m = len(clusters)
    R = []
    for i in range(m):
        all_inds = np.argsort(weights[i])[::-1]
        mask = np.isin(all_inds, clusters[i], invert=True)
        C = all_inds[mask][:n]
        R.append([C, weights[i][C]])
    return R


# Weighing each of the clusters as they fit the full initial set of objects (Method 1).
# The cluster acquires the greatest weight the smaller the average
# distance to all objects in the full initial set of objects.
# Pairwise distances between objects are represented by matrix D.
####################################
# Взвешивание каждого кластера по мере его соответствия полному исходному множеству объектов (Метод 1).
# Кластер получает тем больший вес чем меньше среднее расстояние до всех объектов полного исходного множества.
# Попарные расстояния между объектами представлены матрицей D
def cluster_weighting1(clusters, D):
    import numpy as np
    def weight(cluster, D):
        dm = np.array(D[cluster][:], dtype=np.float)
        mean = np.mean(dm, axis=1)
        mx = np.max(mean)
        if mx > 0:
            norm = 1 - mean / mx
        else:
            norm = np.ones(mean.shape, dtype=np.float)
        return np.mean(norm)

    W = np.array([weight(c, D) for c in clusters])
    maxW = np.max(W)
    if maxW > 0:
        return W / maxW
    else:
        return np.zeros(W.shape, dtype=np.float)


# Weighing each of the clusters as they fit the full initial set of objects (Method 2).
# The cluster acquires the greatest weight the greatest the average
# co-occurrence frequency with all objects in the full initial set of objects.
# Co-occurrence frequencies between objects are represented by matrix cooc.
####################################
# Взвешивание каждого из полученных кластеров по мере
# их соответствия полному исходному множеству объектов (Метод 2).
# Кластер получает тем больший вес чем больше среднее частота встречаемости объектов рассматриваемого кластера с
# со всеми объектами полного исходного множества.
# Попарные частоты встречаимости объектов представлены в матрице cooc.
def cluster_weighting2(clusters, cooc):
    import numpy as np
    def weight(cluster, cooc):
        dm = np.array(cooc[cluster][:], dtype=np.float)
        mean = np.mean(dm, axis=1)
        mx = np.max(mean)
        if mx > 0:
            norm = mean / mx
        else:
            norm = np.zeros(mean.shape, dtype=np.float)
        return np.mean(norm)
    W = np.array([weight(c, cooc) for c in clusters])
    maxW = np.max(W)
    if maxW > 0:
        return W / maxW
    else:
        return np.zeros(W.shape, dtype=np.float)
