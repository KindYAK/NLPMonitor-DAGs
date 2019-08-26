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

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING
    from mainapp.documents import EmbeddingIndex

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    number_of_documents = s.count()

    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_EMBEDDING):
        query = {
            "corpus": "main",
            "number_of_documents": number_of_documents,
            "is_ready": False,
            "name": TOKEN_EMBEDDING_NAME,
        }
        if search(ES_CLIENT, ES_INDEX_EMBEDDING, query):
            return("!!!", "Already exists")

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
                       end=int(end/100*number_of_documents),
                       source=['id', 'text']
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
                            f"{os.path.join(BASE_DAG_DIR, 'dags', 'bert_embeddings', 'bert', 'extract_features.py')}",
                            f"--input_file={os.path.join(tmpdir, input_file_name)}",
                            f"--output_file={os.path.join(tmpdir, output_file_name)}",
                            f"--vocab_file={os.path.join(BASE_DAG_DIR, 'dags', 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'vocab.txt')}",
                            f"--bert_config_file={os.path.join(BASE_DAG_DIR, 'dags', 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'bert_config.json')}",
                            f"--init_checkpoint={os.path.join(BASE_DAG_DIR, 'dags', 'bert_embeddings','bert', 'models', 'rubert_cased_L-12_H-768_A-12_v1', 'bert_model.ckpt')}",
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
                "doc": {TOKEN_EMBEDDING_NAME: embedding},
            }

    for ok, result in streaming_bulk(ES_CLIENT, update_generator(documents, embeddings), index=ES_INDEX_DOCUMENT,
                                     chunk_size=1000, raise_on_error=True, max_retries=10):
        pass


def persist_token_embeddings():
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING
    from mainapp.models import Corpus, Document
    from preprocessing.models import ProcessedCorpus, ProcessedDocument, AnalysisUnit

    from elasticsearch_dsl import Search

    # Get embedding object
    query = {
        "corpus": "main",
        # "is_ready": False,  # TODO uncomment
        "name": TOKEN_EMBEDDING_NAME.lower(),
        "by_unit": "token",
        "algorithm": "BERT".lower(),
        "pooling": "None".lower(),
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    # Update to is_ready
    ES_CLIENT.update(index=ES_INDEX_EMBEDDING, id=embedding.meta.id, body={"doc": {"is_ready": True}})

    # Init processedCorpus
    pcs = ProcessedCorpus.objects.filter(corpus=Corpus.objects.get(name="main"), name=TOKEN_EMBEDDING_NAME)
    if pcs.exists():
        for pc in pcs:
            pc.delete()
    pc = ProcessedCorpus.objects.create(corpus=Corpus.objects.get(name="main"),
                                        name=TOKEN_EMBEDDING_NAME,
                                        description="Bert token embedding using Rubert model from DeepPavlov")

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).source(['id', TOKEN_EMBEDDING_NAME]).filter("term", corpus="main")

    def persist(batch_docs, batch_units):
        batch_docs = ProcessedDocument.objects.bulk_create(batch_docs)
        batch_units_objs = []
        batch_size = 10000
        i = 0
        for doc, embs in zip(batch_docs, batch_units):
            ind = 0
            for emb in embs:
                batch_units_objs.append(AnalysisUnit(type=0,
                                                     processed_document=doc,
                                                     value=emb['token'],
                                                     index=ind,
                                                     embedding=emb['values']
                                                     )
                                        )
                print(i, len(emb['token']))
                i += 1
                ind += 1
            if i >= batch_size:
                AnalysisUnit.objects.bulk_create(batch_units_objs)
                batch_units_objs = []
        print("!!!", len(batch_units_objs))
        AnalysisUnit.objects.bulk_create(batch_units_objs)

    batch_size = 10000
    batch_docs = []
    batch_units = []
    for document in s.scan():
        batch_docs.append(ProcessedDocument(processed_corpus=pc, original_document_id=document.id))
        if TOKEN_EMBEDDING_NAME not in document:
            print("!!!", document.meta.id, document.id, "Skipped")
            continue
        else:
            print("!!!", "OK")
        embeddings = document[TOKEN_EMBEDDING_NAME]
        document_embeddings = []
        for sent in embeddings:
            for token in sent:
                document_embeddings.append(
                    {
                        "token": token.token,
                        "values": token.layers[0].values
                    }
                )
        batch_units.append(document_embeddings)

        if len(batch_docs) >= batch_size:
            persist(batch_docs, batch_units)
            batch_docs = []
            batch_units = []
    persist(batch_docs, batch_units)
