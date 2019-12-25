def normalize_topic_documnets(buckets, total_metrics_dict):
    for bucket in buckets:
        total_weight = total_metrics_dict[bucket.key_as_string]['weight']
        total_size = total_metrics_dict[bucket.key_as_string]['size']
        if total_weight != 0:
            bucket.dynamics_weight.value /= total_weight
        if total_size != 0:
            bucket.doc_count_normal = bucket.doc_count / total_size
        else:
            bucket.doc_count_normal = 0


def calc_topics_info(corpus, topic_modelling_name, topic_weight_threshold):
    import datetime
    from statistics import mean, median, pstdev

    from elasticsearch_dsl import Search
    from mainapp.services import apply_fir_filter
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_TOPIC_DOCUMENT
    from topicmodelling.services import get_total_metrics

    from dags.bigartm.services.service import TMNotFoundException
    from util.util import geometrical_mean
    from .service import get_tm_index

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    if not ES_CLIENT.indices.exists(f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling_name}"):
        return "No TM index ready"

    topic_modelling = get_tm_index(name=topic_modelling_name, corpus=corpus)
    total_metrics_dict = get_total_metrics(topic_modelling_name, "1d", topic_weight_threshold)

    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling_name}")
    std = std.filter("range", topic_weight={"gte": topic_weight_threshold}) \
              .filter("range", datetime={"gte": datetime.date(2000, 1, 1)}) \
              .source([])[:0]
    std.aggs.bucket(name="topics",
                    agg_type="terms",
                    field="topic_id") \
            .bucket(name="dynamics",
                    agg_type="date_histogram",
                    field="datetime",
                    calendar_interval="1d") \
            .metric("dynamics_weight", agg_type="sum", field="topic_weight")
    topics_documents = std.execute()
    topics_documents_dict = dict((bucket.key, bucket.dynamics.buckets)
                                 for bucket in topics_documents.aggregations.topics.buckets)

    for topic in topic_modelling.topics:
        if not topic.id in topics_documents_dict:
            continue
        normalize_topic_documnets(topics_documents_dict[topic.id], total_metrics_dict)

        # Separate signals
        date_ticks = [bucket.key_as_string for bucket in topics_documents_dict[topic.id]]
        absolute_power = [bucket.doc_count for bucket in topics_documents_dict[topic.id]]
        relative_power = [bucket.doc_count_normal for bucket in topics_documents_dict[topic.id]]
        relative_weight = [bucket.dynamics_weight.value for bucket in topics_documents_dict[topic.id]]

        # Smooth
        absolute_power = apply_fir_filter(absolute_power, granularity="1d")
        relative_power = apply_fir_filter(relative_power, granularity="1d")
        relative_weight = apply_fir_filter(relative_weight, granularity="1d")

        # Get topic info metrics
        if len(relative_weight) == 0:
            continue
        topic.weight_mean = mean(relative_weight)
        topic.weight_geom_mean = geometrical_mean(relative_weight)
        topic.weight_std = pstdev(relative_weight)

        periods = []
        periods_maxes = []
        period_start = None
        max_up = None
        is_up = False
        for i, weight in enumerate(relative_weight):
            if weight > topic.weight_geom_mean and not is_up:
                is_up = True
                period_start = i
            elif weight < topic.weight_geom_mean and is_up:
                if max_up - topic.weight_mean > 0.5 * topic.weight_std:
                    periods.append(i - period_start)
                    periods_maxes.append(max_up)
                is_up = False
                max_up = None
            if is_up and (max_up is None or max_up < weight):
                max_up = weight
        if len(periods) == 0:
            continue
        topic.period_num = len(periods)
        topic.period_mean = mean(periods)
        topic.period_std = pstdev(periods)
        topic.period_maxes_mean = mean(periods_maxes)

    def aggregate_stuff(topics, function, field_name):
        data = [getattr(topic, field_name) for topic in topics if hasattr(topic, field_name)]
        if not data:
            return None
        return function(data)

    topic_modelling.period_median = aggregate_stuff(topic_modelling.topics, median, "period_mean")
    topic_modelling.period_maxes_mean_median = aggregate_stuff(topic_modelling.topics, median, "period_maxes_mean")
    topic_modelling.weight_std_median = aggregate_stuff(topic_modelling.topics, median, "weight_std")

    topic_modelling.period_std = aggregate_stuff(topic_modelling.topics, pstdev, "period_mean")
    topic_modelling.period_maxes_mean_std = aggregate_stuff(topic_modelling.topics, pstdev, "period_maxes_mean")
    topic_modelling.weight_std_std = aggregate_stuff(topic_modelling.topics, pstdev, "weight_std")
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=topic_modelling.meta.id, body={"doc": topic_modelling.to_dict()})
    return f"Topics periods info calculated - average periods per topic - {aggregate_stuff(topic_modelling.topics, mean, 'period_num')}\n" \
           f"Number of topics with periods - {len([getattr(topic, 'period_num') for topic in topic_modelling.topics if hasattr(topic, 'period_num')])}\n" \
           f"Median period length - {topic_modelling.period_median}\n"
