def fill_evaldict(dictionary, counter, s, topic_id, topic_modelling_name, value, criterion_id, criterion_name):
    from mainapp.documents import EvalDict

    for word_dict in s[topic_id]:  # TODO scale weighted_criterion_value
        word = word_dict['word']
        weight = word_dict['weight']
        dictionary[counter] = EvalDict(
            topic_modelling_name=topic_modelling_name,
            topic_id=topic_id,
            weighted_criterion_value=weight * value,
            criterion_id=criterion_id,
            word=word,
            criterion_name=criterion_name
        )
        counter += 1

    return counter


def scale_scores(dict_to_scale, scaler):
    import numpy as np

    scores_normalized = [score[0] for score in scaler.fit_transform(np.array([d.weighted_criterion_value for d in
                                                                              dict_to_scale.values()]).reshape(-1, 1))]
    for key in dict_to_scale.keys():
        dict_to_scale[key].weighted_criterion_value = scores_normalized[key]
    return dict_to_scale


def dict_generator(**kwargs):
    topic_modellings_list = kwargs['topic_modellings_list']

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_INDEX_TOPIC_MODELLING, ES_CLIENT
    from elasticsearch_dsl import Search
    from sklearn.preprocessing import MinMaxScaler
    from collections import OrderedDict

    scaler_positive = MinMaxScaler(feature_range=(0.000001, 1))
    scaler_negative = MinMaxScaler(feature_range=(-1, -0.000001))

    for topic_modelling_name in topic_modellings_list:
        s = {element['id']: element['topic_words'] for element in
             Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING).filter('term',
                                                                            **{'name': topic_modelling_name}).source(
                 ['topics']).execute()[0].to_dict()['topics']}

        positive_id = 0
        negative_id = 0
        dict_to_scale_positive, dict_to_scale_negative = OrderedDict(), OrderedDict()
        for crit in EvalCriterion.objects.all():
            criterion_id = crit.id
            criterion_name = crit.name
            for topic in TopicsEval.objects.filter(criterion__id=criterion_id) \
                    .filter(topics__topic_modelling_name=topic_modelling_name):
                value = topic.value
                topic_id = topic.topics.first().topic_id
                if value > 0:
                    positive_id = fill_evaldict(dictionary=dict_to_scale_positive, counter=positive_id, s=s,
                                                topic_id=topic_id,
                                                topic_modelling_name=topic_modelling_name, value=value,
                                                criterion_id=criterion_id, criterion_name=criterion_name)
                elif value < 0:
                    negative_id = fill_evaldict(dictionary=dict_to_scale_negative, counter=negative_id, s=s,
                                                topic_id=topic_id,
                                                topic_modelling_name=topic_modelling_name, value=value,
                                                criterion_id=criterion_id, criterion_name=criterion_name)
                else:
                    continue
        dict_scaled_negative = scale_scores(dict_to_scale=dict_to_scale_negative, scaler=scaler_negative)
        dict_scaled_positive = scale_scores(dict_to_scale=dict_to_scale_positive, scaler=scaler_positive)

        for dicts in [dict_scaled_positive, dict_scaled_negative]:
            for value in dicts.values():
                yield value.to_dict()


def calc_eval_dicts(**kwargs):
    from elasticsearch.helpers import parallel_bulk
    import datetime
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_EVAL_DICT

    total_created = 0
    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, dict_generator(**kwargs),
                                    index=ES_INDEX_EVAL_DICT,
                                    chunk_size=10000,
                                    raise_on_error=True,
                                    thread_count=4):

        if (failed + success) % 2500 == 0:
            print(f"!!!{failed+success} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")
        if not ok:
            failed += 1
        else:
            success += 1
            total_created += 1
