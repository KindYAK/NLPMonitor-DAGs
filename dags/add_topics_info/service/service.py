import datetime


def add_topics_info(**kwargs):
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING

    s = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING)
    # s = s.exclude('exists', field="has_topic_info").filter('term', is_ready=True)
    # s = s.exclude('exists', field="has_topic_info") # TODO Return back

    for i, tm in enumerate(s.scan()):
        print("!!!", f"TopicModelling {i}", datetime.datetime.now())
        add_topic_names(tm)
        calc_topic_size(tm)
        ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=tm.meta.id, body={"doc": {"has_topic_info": True}})
        # TODO Finalize topic info adding


def add_topic_names(tm):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING

    for topic in tm.topics:
        sorted_words = sorted(topic.topic_words, key=lambda x: x.weight, reverse=True)
        topic.name = ", ".join([w.word for w in sorted_words[:5]])
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=tm.meta.id, body={"doc": tm.to_dict()})


def calc_topic_size(tm):
    from elasticsearch_dsl import Search
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_MODELLING, ES_INDEX_DOCUMENT

    for topic in tm.topics:
        topic.topic_size = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)\
                                .filter("term", **{f"topics_{tm.name}.topic": topic.id}).count()
    ES_CLIENT.update(index=ES_INDEX_TOPIC_MODELLING, id=tm.meta.id, body={"doc": tm.to_dict()})
