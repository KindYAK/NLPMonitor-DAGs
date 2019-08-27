from util.service_es import search
from util.constants import BASE_DAG_DIR

import datetime
import json
import os
import tempfile
import subprocess

from dags.bert_embeddings.gen_embed.service_es import persist_embeddings_to_es


TOKEN_EMBEDDING_NAME = "Bert_Token_Embedding_rubert_cased_L_12_H_768_A_12_v1"
WORD_EMBEDDING_NAME = "Bert_Word_Average_Embedding_rubert_cased_L_12_H_768_A_12_v1"
SENTENCE_EMBEDDING_NAME = "Bert_Sentence_Average_Embedding_rubert_cased_L_12_H_768_A_12_v1"
TEXT_EMBEDDING_NAME = "Bert_Text_Average_Max_Embedding_rubert_cased_L_12_H_768_A_12_v1"
def init_embedding_index(**kwargs):
    from elasticsearch_dsl import Search

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING
    from mainapp.documents import EmbeddingIndex

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT)
    number_of_documents = s.count()

    # Check if already exists
    if ES_CLIENT.indices.exists(ES_INDEX_EMBEDDING):
        query = {
            "corpus": kwargs['corpus'],
            "name": kwargs['name'],
            "number_of_documents": number_of_documents,
        }
        if search(ES_CLIENT, ES_INDEX_EMBEDDING, query):
            return ("!!!", "Already exists")

    kwargs["number_of_documents"] = number_of_documents
    index = EmbeddingIndex(**kwargs)
    index.save()


def generate_token_embeddings(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    start = kwargs['start']
    end = kwargs['end']

    # Get embedding object
    query = {
        "corpus": "main",
        # "is_ready": False, # TODO Uncomment
        "name": TOKEN_EMBEDDING_NAME.lower(),
        "by_unit": "token",
        "algorithm": "BERT".lower(),
        "pooling": "None".lower(),
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    number_of_documents = embedding['number_of_documents']

    # Get documents
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT,
                       {"corpus": "main"},
                       start=int(start/100*number_of_documents),
                       end=int(end/100*number_of_documents),
                       source=['id', 'text'],
                       sort=['id']
                       )

    # Embeddings themselves
    from textblob import TextBlob
    embeddings = []
    documents_to_write = []
    input_file_name = f"input-{start}-{end}.txt"
    output_file_name = f"output-{start}-{end}.json"
    batch_size = 10000
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
            documents_to_write.append(document)
            if len(embeddings) >= batch_size:
                persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings, TOKEN_EMBEDDING_NAME)
                embeddings = []
                documents_to_write = []
        persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings, TOKEN_EMBEDDING_NAME)


def persist_embeddings(**kwargs):
    print("!!!", "new2")
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING
    from mainapp.models import Corpus, Document
    from preprocessing.models import ProcessedCorpus, ProcessedDocument, AnalysisUnit

    from elasticsearch_dsl import Search

    corpus = kwargs['corpus']
    embedding_name = kwargs['embedding_name']
    by_unit = kwargs['by_unit']
    type_unit_int = kwargs['type_unit_int']
    algorithm = kwargs['algorithm']
    pooling = kwargs['pooling']
    description = kwargs['description']

    # Update embedding object to is_ready
    query = {
        "corpus": corpus.lower(),
        "name": embedding_name.lower(),
        "by_unit": by_unit.lower(),
        "algorithm": algorithm.lower(),
        "pooling": pooling.lower(),
        # "is_ready": False,  # TODO uncomment
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    ES_CLIENT.update(index=ES_INDEX_EMBEDDING, id=embedding.meta.id, body={"doc": {"is_ready": True}})

    # Init processedCorpus
    pcs = ProcessedCorpus.objects.filter(corpus=Corpus.objects.get(name=corpus), name=TOKEN_EMBEDDING_NAME)
    if pcs.exists():
        for pc in pcs:
            pc.delete()
    pc = ProcessedCorpus.objects.create(corpus=Corpus.objects.get(name=corpus),
                                        name=embedding_name,
                                        description=description)

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).source(['id', embedding_name]).filter("term", corpus=corpus)
    print("!!!", s.count())

    def persist(batch_docs, batch_units, type):
        batch_docs = ProcessedDocument.objects.bulk_create(batch_docs)
        batch_units_objs = []
        batch_size = 10000
        for doc, embs in zip(batch_docs, batch_units):
            ind = 0
            for emb in embs:
                batch_units_objs.append(AnalysisUnit(type=type,
                                                     processed_document=doc,
                                                     value=emb['token'],
                                                     index=ind,
                                                     embedding=emb['values']
                                                     )
                                        )
                ind += 1
            if len(batch_units_objs) >= batch_size:
                AnalysisUnit.objects.bulk_create(batch_units_objs)
                batch_units_objs = []
        AnalysisUnit.objects.bulk_create(batch_units_objs)

    batch_size = 10000
    batch_docs = []
    batch_units = []
    for document in s.scan():
        if embedding_name not in document:
            print("!!!", document.meta.id, document.id, "Skipped")
            continue
        else:
            print("!!!", "OK")
        batch_docs.append(ProcessedDocument(processed_corpus=pc, original_document_id=document.id))
        embeddings = document[embedding_name]
        document_embeddings = []
        if type_unit_int in [0, 1, 2]:
            for sent in embeddings:
                for token in sent:
                    document_embeddings.append(
                        {
                            by_unit: token[by_unit],
                            "values": token.layers[0].values
                        }
                    )
        else:
            for elem in embeddings:
                document_embeddings.append(
                    {
                        by_unit: elem[by_unit],
                        "values": elem.layers[0].values
                    }
                )
        batch_units.append(document_embeddings)
        if len(batch_docs) >= batch_size:
            persist(batch_docs, batch_units, type_unit_int)
            batch_docs = []
            batch_units = []
    persist(batch_docs, batch_units, type=type_unit_int)
