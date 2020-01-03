def evaluate(**kwargs):
    import datetime

    from evaluation.models import EvalCriterion, TopicsEval
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_DOCUMENT_EVAL, ES_INDEX_TOPIC_DOCUMENT, ES_HOST
    from mainapp.documents import DocumentEval

    from elasticsearch_dsl import Search, Index
    from elasticsearch.helpers import parallel_bulk

    import logging
    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.ERROR)

    print("!!!", "Forming topic-eval dict", datetime.datetime.now())
    topic_modelling = kwargs['topic_modelling']
    perform_actualize = 'perform_actualize' in kwargs
    criterion = EvalCriterion.objects.get(id=kwargs['criterion_id'])
    evaluations = TopicsEval.objects.filter(criterion=criterion, topics__topic_modelling_name=topic_modelling).distinct().prefetch_related('topics')

    # Topic -> [List of evaluations by each author]
    criterions_evals_dict = {}
    for evaluation in evaluations:
        if not evaluation.topics.exists():
            continue
        eval_topic_id = evaluation.topics.first().topic_id
        if eval_topic_id not in criterions_evals_dict:
            criterions_evals_dict[eval_topic_id] = []
        criterions_evals_dict[eval_topic_id].append(evaluation.value)

    for t in criterions_evals_dict.keys():
        criterions_evals_dict[t] = sum(criterions_evals_dict[t]) / len(criterions_evals_dict[t])

    # Eval documents
    # Dict Document -> [topic_weight*topic_eval for ...]
    std = Search(using=ES_CLIENT, index=f"{ES_INDEX_TOPIC_DOCUMENT}_{topic_modelling}")
    std = std.filter("range", topic_weight={"gte": 0.001}) \
              .source(['document_es_id', 'topic_weight', 'topic_id', "datetime", "document_source"])

    ids_to_skip = None
    if perform_actualize:
        print("!!!", "Performing actualizing, skipping document already in TM")
        if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}"):
            return "Evaluation doesn't exist yet"
        s = Search(using=ES_CLIENT, index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}").source([])[:0]
        s.aggs.bucket(name="ids", agg_type="terms", field="document_es_id", size=5000000)
        r = s.execute()
        ids_to_skip = set([bucket.key for bucket in r.aggregations.ids.buckets])

    def doc_eval_generator(std):
        current_doc = {}
        yielded_ids = set()
        for td in std.sort('document_es_id').params(preserve_order=True).scan():
            if ids_to_skip is not None and td.document_es_id in ids_to_skip:
                continue
            if td.topic_id not in criterions_evals_dict:
                criterion_value = 0
            else:
                criterion_value = criterions_evals_dict[td.topic_id]
            if current_doc and current_doc['document_es_id'] == td.document_es_id:
                current_doc["value"].append(
                    {
                        "topic_weight": td.topic_weight,
                        "criterion_value": criterion_value,
                        "topic_id": td.topic_id,
                    }
                )
            else:
                if current_doc:
                    if current_doc['document_es_id'] in yielded_ids:
                        raise Exception("!!! Doc eval generator uniquness/sorted problem")
                    yielded_ids.add(current_doc['document_es_id'])
                    yield current_doc
                current_doc = {
                    "value": [
                        {
                            "topic_weight": td.topic_weight,
                            "criterion_value": criterion_value,
                            "topic_id": td.topic_id,
                        }
                    ],
                    "document_es_id": td.document_es_id,
                    "document_datetime": td.datetime if hasattr(td, "datetime") and td.datetime else None,
                    "document_source": td.document_source
                }

    print("!!!", "Sending to elastic + calculating through generators", datetime.datetime.now())
    # Send to elastic
    if not perform_actualize:
        es_index = Index(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", using=ES_CLIENT)
        es_index.delete(ignore=404)
    if not ES_CLIENT.indices.exists(f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}"):
        ES_CLIENT.indices.create(index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}", body={
            "settings": DocumentEval.Index.settings,
            "mappings": DocumentEval.Index.mappings
        }
        )

    def eval_calc_generator(std):
        range_center = (criterion.value_range_from + criterion.value_range_to) / 2
        neutral_neighborhood = 0.1
        for doc in doc_eval_generator(std):
            eval = DocumentEval()
            val = sum([v['topic_weight']*v['criterion_value'] for v in doc["value"]]) / \
                  sum([v['topic_weight'] for v in doc["value"]])

            eval.topic_ids_top = sorted([v for v in doc["value"]
                                  if v['topic_weight']*v['criterion_value'] >= range_center + neutral_neighborhood],
                                        key=lambda v: v['topic_weight']*v['criterion_value'], reverse=True)[:3]
            eval.topic_ids_top = [v['topic_id'] for v in eval.topic_ids_top]
            eval.topic_ids_bottom = sorted([v for v in doc["value"]
                                  if v['topic_weight']*v['criterion_value'] <= range_center - neutral_neighborhood],
                                        key=lambda v: v['topic_weight']*v['criterion_value'])[:3]
            eval.topic_ids_bottom = [v['topic_id'] for v in eval.topic_ids_bottom]

            eval.value = val
            eval.document_es_id = doc["document_es_id"]
            eval.document_datetime = doc["document_datetime"]
            eval.document_source = doc["document_source"]
            yield eval.to_dict()

    total_created = 0
    failed = 0
    success = 0
    for ok, result in parallel_bulk(ES_CLIENT, eval_calc_generator(std),
                                     index=f"{ES_INDEX_DOCUMENT_EVAL}_{topic_modelling}_{criterion.id}",
                                     chunk_size=50000, raise_on_error=True, thread_count=6):
        if not ok:
            failed += 1
        else:
            success += 1
            total_created += 1
        if (failed+success) % 50000 == 0:
            print(f"!!!{failed+success} processed", datetime.datetime.now())
        if failed > 5:
            raise Exception("Too many failed ES!!!")

    if perform_actualize and total_created == 0:
        return f"No documents to actualize"
    return total_created
