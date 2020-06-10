def calc_mma(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING
    from elasticsearch_dsl import Search
    import numpy as np
    from .util import calc_p1, calc_p2, calc_p4, calc_p5, calc_p6, create_delete_index, bulk_factory

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    topic_modellings_list = kwargs['topic_modellings_list']
    criterion_ids_list = kwargs['criterion_ids_list']
    class_ids_list = kwargs['class_ids_list']
    perform_actualize = kwargs['perform_actualize']

    for topic_modelling_name, criterion_ids, c_ids in zip(topic_modellings_list, criterion_ids_list, class_ids_list):
        # Тональность
        # 1
        # Образование
        # 6
        # Здравоохранение
        # 7
        # Культура
        # и
        # духовность
        # 8
        # Социальная
        # защита
        # 9
        # Правопорядок
        # 10
        # Благополучие(Экономика)
        # 11
        # Международные
        # отношения
        # 12
        # Внутренняя
        # политика
        # 13
        # Наука
        # и
        # инновации
        # 14
        # TODO use this criterion ids in production: 1-тональность, 33-объективность, 35-резонансность, 32-пропаганда
        tm = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING).filter('term', **{'name': topic_modelling_name}) \
            .source(['number_of_topics']).execute()[0]
        topics_number = tm.number_of_topics
        p1_matrix = calc_p1(topic_modelling_name=topic_modelling_name,
                            criterion_ids=criterion_ids,
                            topics_number=topics_number)

        p2_matrix, document_es_guide = calc_p2(topic_modelling_name=topic_modelling_name,
                                               topics_number=topics_number)

        # TODO Move to op_kwargs
        p3_matrix = np.array([0.44, 0.33, 0.23]).reshape(-1, 1)  # TODO embedding for class tonalnost, rezonansnost, gos

        p4_matrix = calc_p4(p1=p1_matrix, p3=p3_matrix)  # prob x weight

        p5_matrix = calc_p5(p2=p2_matrix, p4=p4_matrix)  # weight x prob

        p6_matrix = calc_p6(p1=p1_matrix, p2=p2_matrix)  # weight x prob

        index_kwargs = {
            'perform_actualize': perform_actualize,
            'topic_modelling_name': topic_modelling_name,
            'scored_documents': p6_matrix,
            'is_criterion': True,
            'crit_or_class_ids': criterion_ids,
            'document_es_guide': document_es_guide
        }

        create_delete_index(**index_kwargs)
        bulk_factory(**index_kwargs)

        index_kwargs['crit_or_class_ids'] = c_ids
        index_kwargs['is_criterion'] = False
        index_kwargs['scored_documents'] = p5_matrix
        create_delete_index(**index_kwargs)
        bulk_factory(**index_kwargs)

    return 'Done'
