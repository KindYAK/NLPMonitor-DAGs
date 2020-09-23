def calc_mma(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING
    from elasticsearch_dsl import Search
    import numpy as np
    from .util import calc_p1, calc_p2, calc_p4, calc_p5, calc_p6, create_delete_index, bulk_factory

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    topic_modelling_name = kwargs['topic_modelling_name']
    criterion_ids = kwargs['criterion_ids']
    criterion_weights = kwargs['criterion_weights']
    class_ids = kwargs['class_ids']
    perform_actualize = kwargs['perform_actualize']

    tm = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING).filter('term', **{'name': topic_modelling_name}) \
        .source(['number_of_topics']).execute()[0]
    topics_number = tm.number_of_topics
    p1_matrix = calc_p1(topic_modelling_name=topic_modelling_name,
                        criterion_ids=criterion_ids,
                        topics_number=topics_number)
    p2_matrix, document_es_guide = calc_p2(topic_modelling_name=topic_modelling_name,
                                           topics_number=topics_number)
    p3_matrix = np.array(criterion_weights).reshape(-1, 1)
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

    index_kwargs['crit_or_class_ids'] = class_ids
    index_kwargs['is_criterion'] = False
    index_kwargs['scored_documents'] = p5_matrix
    create_delete_index(**index_kwargs)
    bulk_factory(**index_kwargs)

    return 'Done'
