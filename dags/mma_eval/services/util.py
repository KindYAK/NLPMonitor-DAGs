def calc_p1(topic_modelling_name, criterion_ids, topics_number):
    from sklearn.preprocessing import MinMaxScaler
    from datetime import datetime
    from evaluation.models import TopicIDEval
    from collections import defaultdict, OrderedDict
    import numpy as np

    scaler = MinMaxScaler(feature_range=(0, 1))
    print('!!! p1 matrix calculating started', datetime.now())
    p1_matrix = None
    for crit_id in criterion_ids:
        column = defaultdict(list, {i: [0] for i in range(topics_number)})
        evals = TopicIDEval.objects.filter(topics_eval__criterion__id=crit_id,
                                           topic_id__topic_modelling_name=topic_modelling_name)
        if not evals:
            continue

        for eval_ in evals:
            value = eval_.topics_eval.value
            topic_id = int(eval_.topic_id.topic_id.split('_')[1])
            column[topic_id].append(value)

        for key, value in column.items():
            column[key] = np.mean(value)

        ordered_column = OrderedDict(sorted(column.items()))
        if crit_id == 1:
            for key, value in ordered_column.items():
                ordered_column[key] = -value

        column = scaler.fit_transform(np.array(list(ordered_column.values())).reshape(-1, 1))
        if p1_matrix is None:
            p1_matrix = column
            continue
        p1_matrix = np.hstack((p1_matrix, column))
    print('!!! p1 matrix calculated', p1_matrix.shape, datetime.now())
    return p1_matrix


def calc_p2(topic_modelling_name, topics_number):
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_TOPIC_DOCUMENT
    from datetime import datetime
    import numpy as np
    from collections import defaultdict

    print('!!! p2 matrix calculating started', datetime.now())

    theta = Search(using=ES_CLIENT, index=f'{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling_name}') \
        .source(['document_es_id', 'datetime', 'document_source', 'topic_weight', 'topic_id'])

    theta_dict = defaultdict(list)
    document_es_ids = dict()
    total = theta.count()
    for i, t in enumerate(theta.scan()):
        if i % 10_000_000 == 0:
            print(f'!!! {i}/{total} thetas passed in dict creating')
        theta_dict[t.document_es_id].append([t.topic_id, t.topic_weight])
        if t.document_es_id not in document_es_ids.keys():
            document_es_ids[t.document_es_id] = {'datetime': getattr(t, "datetime", None),
                                                 'document_source': getattr(t, 'document_source', None)}

    total = len(document_es_ids)
    p2_matrix = np.zeros((total, topics_number))
    for i, document_id in enumerate(document_es_ids):
        if i % 100_000 == 0:
            print(f'!!! {i}/{total} documents passed in p2 matrix creating')
        column = np.zeros(topics_number)
        for topic_doc in theta_dict[document_id]:
            id_in_column = int(topic_doc[0].split('_')[1])
            column[id_in_column] = topic_doc[1]
        p2_matrix[i] = column
    print('!!! p2 matrix calculated', p2_matrix.shape, datetime.now())
    return p2_matrix, document_es_ids


def calc_p4(p1, p3):
    from datetime import datetime
    print('!!! p4 matrix calculating started', datetime.now())
    """Вероятность совпадения тематики и класса: p4[k][c]"""
    p4 = custom_dot(matrix_1=p3.T, matrix_2=p1.T, agg_type='bayes')
    print('!!! p4 matrix calculated', p4.T.shape, datetime.now())
    return p4.T


def calc_p5(p4, p2):
    from datetime import datetime
    print('!!! p5 matrix calculating started', datetime.now())
    """Распределение вероятностей статей по классам: p5 [m][c]"""
    p5 = custom_dot(matrix_1=p2, matrix_2=p4, agg_type='bayes')
    print('!!! p5 matrix calculated', p5.shape, datetime.now())
    return p5


def calc_p6(p1, p2):
    from datetime import datetime
    print('!!! p6 matrix calculating started', datetime.now())
    """Распределение статей по признакам"""
    p6 = custom_dot(matrix_1=p2, matrix_2=p1, agg_type='bayes')
    print('!!! p6 matrix calculated', p6.shape, datetime.now())
    return p6


def create_delete_index(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT_EVAL
    from mainapp.documents import DocumentEval
    from util.util import shards_mapping
    from elasticsearch_dsl import Index

    crit_or_class_ids = kwargs['crit_or_class_ids']
    is_criterion = kwargs['is_criterion']
    perform_actualize = kwargs['perform_actualize']
    topic_modelling_name = kwargs['topic_modelling_name']
    scored_documents = kwargs['scored_documents']

    for crit_id in crit_or_class_ids:
        if not perform_actualize:
            es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling_name}_{crit_id}{'_m4a' if is_criterion else '_m4a_class'}", using=ES_CLIENT)
            es_index.delete(ignore=404)
        if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling_name}_{crit_id}{'_m4a' if is_criterion else '_m4a_class'}"):
            settings = DocumentEval.Index.settings
            settings['number_of_shards'] = shards_mapping(scored_documents.shape[0])
            ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling_name}_{crit_id}{'_m4a' if is_criterion else '_m4a_class'}", body={
                "settings": settings,
                "mappings": DocumentEval.Index.mappings
                }
            )


def bulk_factory(**kwargs):
    from elasticsearch.helpers import parallel_bulk
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT_EVAL
    from datetime import datetime

    crit_or_class_ids = kwargs['crit_or_class_ids']
    scored_documents = kwargs['scored_documents']
    is_criterion = kwargs['is_criterion']
    topic_modelling_name = kwargs['topic_modelling_name']
    perform_actualize = kwargs['perform_actualize']
    document_es_guide = kwargs['document_es_guide']
    print(f'!!! start elastic sending for {"criteria" if is_criterion else "classes"}', datetime.now())
    for i, ids in enumerate(crit_or_class_ids):
        total_created = 0
        failed = 0
        success = 0
        for ok, result in parallel_bulk(ES_CLIENT, document_eval_generator(class_crit=scored_documents,
                                                                           document_guide=document_es_guide,
                                                                           enum_id=i),
                                        index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling_name}_{ids}{'_m4a' if is_criterion else '_m4a_class'}",
                                        chunk_size=10000 if not perform_actualize else 500, raise_on_error=True,
                                        thread_count=4):
            if (failed + success) % 100_000 == 0:
                print(f"!!!{failed+success}/{len(scored_documents)} processed", datetime.now())
            if failed > 5:
                raise Exception("Too many failed ES!!!")
            if not ok:
                failed += 1
            else:
                success += 1
                total_created += 1


def document_eval_generator(class_crit, document_guide, enum_id):
    from mainapp.documents import DocumentEval

    for i, document_es_id in enumerate(document_guide.keys()):
        bayes_value = class_crit[i, enum_id]
        doc = DocumentEval(value=bayes_value,
                           document_es_id=document_es_id,
                           document_datetime=document_guide[document_es_id]['datetime'],
                           document_source=document_guide[document_es_id]['document_source'])

        yield doc.to_dict()


def bayes(values):
    """
    :param values:
    :return:
    """
    hypothesis = 0.5
    for val in values:
        hypothesis = val * hypothesis / (val * hypothesis + (1 - val) * (1 - hypothesis))
    return hypothesis


def custom_dot(matrix_1, matrix_2, agg_type='mean'):
    import numpy as np
    """
    1.берем строку м1 берем столбец м2
    2.попарное умножение со "стагияванием"
    стягивание это - оценка значений столбца и строки (столбец это вероятность, строка это вес)
    логика стягивания - threshold = 0.5, (P-0.5) * w + 0.5
    3.логика агрегации вероятностей ??? среднее
    4.шкалирование по матрице ,если меньше 0.5 одна своя шкала, если 0.5 то другая своя шкала
    """
    new_matrix_rows = matrix_1.shape[0]
    new_matrix_cols = matrix_2.shape[1]
    new_matrix = np.zeros(shape=(new_matrix_rows, new_matrix_cols))
    for index, weights in enumerate(matrix_1):
        for col in range(new_matrix_cols):
            probs = matrix_2[:, col]
            assert len(probs) == len(weights)
            values = [(p - 0.5) * w + 0.5 + 2 ** -20 for p, w in zip(probs, weights)]  # 2 ** -20 bcs of prob 0 issue
            if agg_type == 'mean':
                new_matrix_element = np.mean(values)
            elif agg_type == 'bayes':
                new_matrix_element = bayes(values)
            else:
                new_matrix_element = sum(values)

            new_matrix[index, col] = new_matrix_element

    if agg_type == 'bayes':
        return new_matrix

    min_low, max_low, min_up, max_up = 1, 0.5, 1, 0.5

    for row in new_matrix:
        for col_elem in row:
            if col_elem < 0.5:
                if col_elem < min_low:
                    min_low = col_elem
                if col_elem > max_low:
                    max_low = col_elem
            else:
                if col_elem < min_low:
                    min_low = col_elem
                if col_elem > max_low:
                    max_low = col_elem

    for index, row in enumerate(new_matrix):
        for col, col_elem in enumerate(row):
            if col_elem <= 0.5:
                new_matrix[index, col] = 0.5 * (col_elem - min_low) / (max_low - min_low)
            else:
                new_matrix[index, col] = 0.5 * (col_elem - min_up) / (max_up - min_up) + 0.5

    return new_matrix
