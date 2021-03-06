def find_combos(**kwargs):
    import datetime
    import itertools
    from collections import defaultdict

    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Index, Search
    from mainapp.documents import TopicCombo
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_DOCUMENT, ES_INDEX_TOPIC_COMBOS, ES_INDEX_TOPIC_MODELLING

    from util.util import shards_mapping, jaccard_similarity

    # #################### INIT ##########################################
    print("!!!", "Init start", datetime.datetime.now())
    topic_modelling = kwargs['name']
    topic_weight_threshold = 0.1
    try:
        tm = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING).filter("term", name=topic_modelling).execute()[0]
    except:
        tm = Search(using=ES_CLIENT, index=ES_INDEX_TOPIC_MODELLING).filter("term", **{"name.keyword": topic_modelling}).execute()[0]
    MIN_VOLUME = 1 / tm.number_of_topics / 1
    topic_words_dict = dict(
        (t.id,
             {
                 "words_full": [w.to_dict() for w in t.topic_words],
                 "words": [w['word'] for w in t.topic_words],
                 "name": ", ".join(w['word'] for w in sorted(t.topic_words, key=lambda x: x.weight, reverse=True)[:5])
             }
         ) for t in tm.topics
    )
    jaccard_similarities = [jaccard_similarity(t1['words'], t2['words']) for t1, t2 in itertools.combinations(topic_words_dict.values(), 2)]
    average_jaccard_similarity = sum(jaccard_similarities) / len(jaccard_similarities)
    print("!!!", "Average jaccard", average_jaccard_similarity)

    # #################### COMBINATIONS ##########################################
    print("!!!", "Topic_docs dict start", datetime.datetime.now())
    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}") \
        .filter("range", topic_weight={"gte": topic_weight_threshold}).source(('topic_id', 'document_es_id'))
    n = std.count()

    topic_docs_dict = defaultdict(set)
    overall_docs = set()
    overall_topic_ids = set()
    for i, td in enumerate(std.scan()):
        topic_docs_dict[td.topic_id].add(td.document_es_id)
        overall_docs.add(td.document_es_id)
        overall_topic_ids.add(td.topic_id)
        if i % 10000 == 0:
            print(f"{i}/{n} processed")

    def topic_combo_generator():
        sent_combos = []
        average_topic_len = len(overall_docs) * MIN_VOLUME
        for topic1 in topic_docs_dict.items():
            topic_combinations = []
            for topic2 in topic_docs_dict.items():
                if topic1[0] == topic2[0]:
                    continue
                if jaccard_similarity(topic_words_dict[topic1[0]]['words'], topic_words_dict[topic2[0]]['words']) > average_jaccard_similarity:
                    continue
                common_docs = None
                topic_ids = set()
                for topic_id, docs in [topic1, topic2]:
                    if common_docs is None:
                        common_docs = set(docs)
                    else:
                        common_docs = common_docs.intersection(docs)
                    topic_ids.add(topic_id)
                if len(common_docs) > 0:
                    topic_combinations.append(
                        {
                            "topics": [
                                {
                                    "id": topic_id,
                                    "name": topic_words_dict[topic_id]['name'],
                                    "words": topic_words_dict[topic_id]['words_full'],
                                } for topic_id in topic_ids
                            ],
                            "common_docs_ids": list(common_docs),
                            "common_docs_len": len(common_docs),
                        })
            topic_combinations = sorted(topic_combinations, key=lambda x: x['common_docs_len'], reverse=True)
            sent = 0
            traversed = 0
            while traversed < len(topic_combinations) and (sent < 5 or topic_combinations[traversed]['common_docs_len'] > average_topic_len / 2):
                topic_ids_set = set([topic["id"] for topic in topic_combinations[traversed]["topics"]])
                if topic_ids_set not in sent_combos:
                    yield topic_combinations[traversed]
                    sent += 1
                    sent_combos.append(topic_ids_set)
                traversed += 1

    print("!!!", "Write document-topics", datetime.datetime.now())
    es_index = Index(f"{ES_INDEX_TOPIC_COMBOS}_{topic_modelling}", using=ES_CLIENT)
    es_index.delete(ignore=404)

    if not ES_CLIENT.indices.exists(f"{ES_INDEX_TOPIC_COMBOS}_{topic_modelling}"):
        settings = TopicCombo.Index.settings
        settings['number_of_shards'] = shards_mapping(1000)
        ES_CLIENT.indices.create(index=f"{ES_INDEX_TOPIC_COMBOS}_{topic_modelling}", body={
            "settings": settings,
            "mappings": TopicCombo.Index.mappings
        }
        )

    success, failed = 0, 0
    batch_size = 100
    for ok, result in parallel_bulk(ES_CLIENT, (doc for doc in topic_combo_generator()),
                                    index=f"{ES_INDEX_TOPIC_COMBOS}_{topic_modelling}", chunk_size=batch_size,
                                    thread_count=3,
                                    raise_on_error=True):
        if ok:
            success += 1
        else:
            print("!!!", "ES index fail, error", result)
            failed += 1
        if failed > 3:
            raise Exception("Too many failed to ES!!")
        if (success + failed) % batch_size == 0:
            print(f'{success + failed} processed')
    print("!!!", "Done writing", datetime.datetime.now())
    return success
