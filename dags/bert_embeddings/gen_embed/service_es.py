def update_embedding_generator(index, documents, embeddings, embedding_name):
    for document, embedding in zip(documents, embeddings):
        yield {
            "_index": index,
            "_op_type": "update",
            "_id": document.meta.id,
            "doc": {embedding_name: embedding},
        }


def persist_embeddings_to_es(client, index, documents, embeddings, embedding_name):
    from elasticsearch.helpers import parallel_bulk

    failed = 0
    for ok, result in parallel_bulk(client, update_embedding_generator(index, documents, embeddings, embedding_name),
                                     index=index, chunk_size=2500, thread_count=4, raise_on_error=True):
        if not ok:
            failed += 1
        if failed > 3:
            raise Exception("Too many failed!!")
