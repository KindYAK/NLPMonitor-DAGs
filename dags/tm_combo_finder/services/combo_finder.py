def find_combos(**kwargs):
    import datetime
    import itertools
    from collections import defaultdict

    from elasticsearch.helpers import parallel_bulk
    from elasticsearch_dsl import Index, Search
    from mainapp.documents import TopicCombo
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_TOPIC_DOCUMENT, ES_INDEX_TOPIC_COMBOS

    from util.util import shards_mapping

    # #################### INIT ##########################################
    topic_modelling = kwargs['name']
    topic_weight_threshold = 0.05

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

    topic_combinations = []
    MAX_L = 3
    MIN_VOLUME = 1 / len(overall_topic_ids)
    average_topic_len = len(overall_docs) * MIN_VOLUME
    def topic_combo_generator():
        for L in range(2, MAX_L + 1):
            print(f"L = {L}")
            for topics in itertools.combinations(topic_docs_dict.items(), L):
                if L >= 3 and not any(any(topic_id in c['topic_ids'] for c in topic_combinations) for topic_id, _ in topics):
                    continue
                common_docs = None
                topic_ids = set()
                for topic_id, docs in topics:
                    if common_docs is None:
                        common_docs = set(docs)
                    else:
                        common_docs = common_docs.intersection(docs)
                    topic_ids.add(topic_id)
                if len(common_docs) > average_topic_len / L:
                    topic_combinations.append({
                            "topic_ids": list(topic_ids),
                            "common_docs_ids": list(common_docs),
                            "common_docs_len": len(common_docs),
                        })
                    yield topic_combinations[-1]

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
