
def run_cluster(**kwargs):
    import datetime

    from collections import defaultdict

    from elasticsearch_dsl import Search

    from nlpmonitor.settings import (
        ES_CLIENT,
        ES_INDEX_SOURCE_CLUSTERS,
        ES_INDEX_TOPIC_DOCUMENT,
        ES_INDEX_TOPIC_MODELLING,
    )

    tm_name = kwargs['tm_name']
    eps_range = kwargs['eps_range']
    min_samples_range = kwargs['min_samples_range']
    if not ES_CLIENT.indices.exists(ES_INDEX_SOURCE_CLUSTERS):
        ES_CLIENT.indices.create(index=ES_INDEX_SOURCE_CLUSTERS)
    else:
        Search(using=ES_CLIENT, index=ES_INDEX_SOURCE_CLUSTERS).filter("term", name=tm_name).delete()

    tm_index = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_MODELLING}").filter("term", name=tm_name).execute()[0]
    number_of_topics = tm_index.number_of_topics

    print("!!!", "Getting source_vectors", datetime.datetime.now())
    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{tm_name}")[:0]
    std.aggs.bucket("sources", agg_type="terms", field="document_source", size=1000)
    std.aggs['sources'].bucket("topics", agg_type="terms", field="topic_id", size=1000)
    std.aggs['sources']['topics'].metric("topic_weight", agg_type="sum", field="topic_weight")
    r = std.execute()

    sources_vectors = []
    sources_names = []
    for source in r.aggregations.sources.buckets:
        topic_weight_dict = defaultdict(int)
        for topic in source.topics.buckets:
            topic_weight_dict[topic.key] = topic.topic_weight.value
        sources_vectors.append([topic_weight_dict[f"topic_{i}"] for i in range(number_of_topics)])
        sources_names.append(source.key)

    print("!!!", "Starting clustering", datetime.datetime.now())
    for eps in eps_range:
        for min_samples in min_samples_range:
            print(f"!!! eps={eps} min_samples={min_samples}")
            perform_clustering(tm_name, sources_vectors, sources_names, eps, min_samples)
    return f"Done, {1} clusters"


def perform_clustering(tm_name, sources_vectors, sources_names, eps, min_samples):
    from collections import defaultdict
    from sklearn.cluster import DBSCAN

    from mainapp.documents import ClusterSource

    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric="cosine").fit(sources_vectors)
    clusters = defaultdict(list)
    for source, cluster in zip(sources_names, clustering.labels_):
        clusters[str(cluster)].append(source)

    es_clustering = ClusterSource(name=tm_name, clusters=clusters, clustering_params={
        "eps": eps,
        "min_samples": min_samples
    })
    es_clustering.save()
