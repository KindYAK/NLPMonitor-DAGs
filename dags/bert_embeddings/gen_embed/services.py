from util.service_es import search
from util.constants import BASE_DAG_DIR

import datetime
import json
import os
import tempfile
import subprocess


TOKEN_EMBEDDING_NAME = "Bert_Token_Embedding_rubert_cased_L_12_H_768_A_12_v1"
def init_token_embedding_index():
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT
    from mainapp.documents import EmbeddingIndex

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    number_of_documents = s.count()
    index = EmbeddingIndex(**{
        "corpus": "main",
        "number_of_documents": number_of_documents,
        "is_ready": False,
        "name": TOKEN_EMBEDDING_NAME,
        "description": "Bert token embedding using Rubert model from DeepPavlov",
        "datetime_created": datetime.datetime.now(),
        "by_unit": "token",
        "algorithm": "BERT",
        "pooling": "None",
        "meta_parameters": {
            "pre_trained": "rubert_cased_L-12_H-768_A-12_v1",
        },
    })
    index.save()


def generate_token_embeddings(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    start = kwargs['start']
    end = kwargs['end']

    # Get embedding object
    query = {
        "corpus": "main",
        "is_ready": False,
        "name": TOKEN_EMBEDDING_NAME.lower(),
        "by_unit": "token",
        "algorithm": "BERT".lower(),
        "pooling": "None".lower(),
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    number_of_documents = embedding['number_of_documents']

    # Get documents
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, {"corpus": "main"},
                       start=int(start/100*number_of_documents),
                       end=int(end/100*number_of_documents)
                       )

    # Embeddings themselves
    from textblob import TextBlob
    embeddings = []
    input_file_name = f"input-{start}-{end}.txt"
    output_file_name = f"output-{start}-{end}.json"
    with tempfile.TemporaryDirectory() as tmpdir:
        for document in documents:
            # Write to input.txt
            with open(os.path.join(tmpdir, input_file_name), "w", encoding='utf-8') as f:
                text = TextBlob(document.text)
                for sentence in text.sentences:
                    f.write(sentence.string.replace("\n", " ") + "\n")
            # Run bert
            subprocess.run(["python",
                            f"{os.path.join(BASE_DAG_DIR, 'bert_embeddings', 'bert', 'extract_features.py')}",
                            f"--input_file={os.path.join(tmpdir, input_file_name)}",
                            f"--output_file={os.path.join(tmpdir, output_file_name)}",
                            f"--vocab_file={os.path.join(BASE_DAG_DIR, 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'vocab.txt')}",
                            f"--bert_config_file={os.path.join(BASE_DAG_DIR, 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'bert_config.json')}",
                            f"--init_checkpoint={os.path.join(BASE_DAG_DIR, 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'bert_model.ckpt')}",
                            "--layers=-2",
                            "--max_seq_length=128",
                            "--batch_size=1000"]
                           )

            # Read from output.json
            document_embeddings = []
            with open(os.path.join(tmpdir, output_file_name), "r", encoding='utf-8') as f:
                for line in f.readlines():
                    embedding = json.loads(line)
                    document_embeddings.append(embedding['features'])
            embeddings.append(document_embeddings)

    # Write to ES
    from elasticsearch.helpers import streaming_bulk

    def update_generator(documents, embeddings):
        for document, embedding in zip(documents, embeddings):
            yield {
                "_index": ES_INDEX_DOCUMENT,
                "_op_type": "update",
                "_id": document.meta.id,
                "doc": {f"{TOKEN_EMBEDDING_NAME}": embedding},
            }

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(documents, embeddings), index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        pass


def persist_token_embeddings():
    pass
