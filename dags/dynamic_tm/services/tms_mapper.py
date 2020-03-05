def mapper(**kwargs):
    """
    идем в мета дтм, тянем оттуда дтм для которого хотим получить маппинги потом идем по этому meta_dtm_name тянем все
    топик моделлинги, формируем два листа набора слов, скорим

    """
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DYNAMIC_TOPIC_MODELLING, ES_INDEX_DYNAMIC_TOPIC_DOCUMENT
    from mainapp.documents import Mappings
    from util.service_es import search
    from util.util import parse_topics_field, mapper, validator
    import json

    meta_dtm_name = kwargs['meta_dtm_name']
    datetime_from_tm_1 = kwargs['datetime_from_tm_1']
    datetime_to_tm_1 = kwargs['datetime_to_tm_1']
    datetime_from_tm_2 = kwargs['datetime_from_tm_2']
    datetime_to_tm_2 = kwargs['datetime_to_tm_2']
    number_of_topics = kwargs['number_of_topics']
    theta_name_1 = ES_INDEX_DYNAMIC_TOPIC_DOCUMENT + "_" + kwargs['name_immutable'] + "_" + str(
        datetime_from_tm_1) + "_" + str(datetime_to_tm_1)
    theta_name_2 = ES_INDEX_DYNAMIC_TOPIC_DOCUMENT + "_" + kwargs['name_immutable'] + "_" + str(
        datetime_from_tm_2) + "_" + str(datetime_to_tm_2)
    # TODO fix meta_dtm_name issue
    tm_1 = search(client=ES_CLIENT, index=ES_INDEX_DYNAMIC_TOPIC_MODELLING,
                  query={
                      'meta_dtm_name.keyword': meta_dtm_name,
                      'datetime_from__gte': datetime_from_tm_1,
                      'datetime_to__lte': datetime_to_tm_1
                  },
                  source=['name', 'meta_dtm_name', 'datetime_from', 'datetime_to', 'topics', 'topic_doc'],
                  )

    tm_2 = search(client=ES_CLIENT, index=ES_INDEX_DYNAMIC_TOPIC_MODELLING,
                  query={
                      'meta_dtm_name.keyword': meta_dtm_name,
                      'datetime_from__gte': datetime_from_tm_2,
                      'datetime_to__lte': datetime_to_tm_2
                  },
                  source=['name', 'meta_dtm_name', 'datetime_from', 'datetime_to', 'topics', 'topic_doc'],
                  )

    tm_1_dict, tm_1_name = parse_topics_field(tm_1[0])
    tm_2_dict, tm_2_name = parse_topics_field(tm_2[0])

    topic_modelling_first_from = tm_1_name.split('_')[-2]
    topic_modelling_second_to = tm_2_name.split('_')[-1]

    thresholds = list(map(str, [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]))

    mappings_dict, delta_words_dict, delta_count_dict = mapper(topic_seq_1=tm_1_dict,
                                                               topic_seq_2=tm_2_dict,
                                                               threshold_list=thresholds)

    scores = validator(mappings_dict=mappings_dict,
                       client=ES_CLIENT,
                       index_theta_one=theta_name_1,
                       index_theta_two=theta_name_2,
                       datetime_from_tm_2=datetime_from_tm_2,
                       datetime_to_tm_1=datetime_to_tm_1,
                       number_of_topics=number_of_topics)

    for threshold in thresholds:
        index = Mappings(
                threshold=threshold,
                meta_dtm_name=meta_dtm_name,
                topic_modelling_first=tm_1_name,
                topic_modelling_second=tm_2_name,
                topic_modelling_first_from=topic_modelling_first_from,
                topic_modelling_second_to=topic_modelling_second_to,
                mappings_dict=json.dumps(mappings_dict[threshold]),
                scores_list=scores[threshold],
                delta_words_dict=json.dumps(delta_words_dict[threshold]),
                delta_count_dict=json.dumps(delta_count_dict[threshold]),
        )
        index.save()

    return 'Mapping created'
