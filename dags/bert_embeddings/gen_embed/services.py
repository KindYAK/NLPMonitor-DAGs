from util.service_es import search
from util.constants import BASE_DAG_DIR

import datetime
import json
import os
import tempfile
import subprocess

from dags.bert_embeddings.gen_embed.service_es import persist_embeddings_to_es


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


def generate_word_embeddings(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    start = kwargs['start']
    end = kwargs['end']

    # Get embedding object
    query = {
        "corpus": "main",
        # "is_ready": False, # TODO Uncomment
        "name": WORD_EMBEDDING_NAME.lower(),
        "by_unit": "word",
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
                    tokens = embedding['features']
                    words = []
                    # Pool tokens into words
                    cur_token = ""
                    cur_embed = []
                    for token in tokens[1:-1]:
                        token_str = token['token']
                        token_emb = token['layers'][0]['values']
                        if not cur_token and not cur_embed:
                            cur_token = token_str
                            cur_embed.append(token_emb)
                        elif "##" in token_str:
                            cur_token += token_str.replace("##", "")
                            cur_embed.append(token_emb)
                        else:
                            cur_embed = pool_vectors(cur_embed, "Average")
                            words.append(
                                {
                                    "layers": [
                                        {
                                            "values": cur_embed,
                                            "index": -2
                                        }
                                    ],
                                    "word": cur_token
                                }
                            )
                            cur_token = token_str
                            cur_embed = [token_emb]
                    if cur_token and cur_embed:
                        cur_embed = pool_vectors(cur_embed, "Average")
                        words.append(
                            {
                                "layers": [
                                    {
                                        "values": cur_embed,
                                        "index": -2
                                    }
                                ],
                                "word": cur_token
                            }
                        )
                    document_embeddings.append(words)
            embeddings.append(document_embeddings)
            documents_to_write.append(document)
            if len(embeddings) >= batch_size:
                persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings, WORD_EMBEDDING_NAME)
                embeddings = []
                documents_to_write = []
        persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings, WORD_EMBEDDING_NAME)


def persist_embeddings(**kwargs):
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
        # "is_ready": False,  # TODO uncomment
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    ES_CLIENT.update(index=ES_INDEX_EMBEDDING, id=embedding.meta.id, body={"doc": {"is_ready": True}})

    # Init processedCorpus
    pcs = ProcessedCorpus.objects.filter(corpus=Corpus.objects.get(name=corpus), name=embedding_name)
    if pcs.exists():
        for pc in pcs:
            pc.delete()
    pc = ProcessedCorpus.objects.create(corpus=Corpus.objects.get(name=corpus),
                                        name=embedding_name,
                                        description=description)

    s = Search(using=ES_CLIENT, index=ES_INDEX_DOCUMENT).source(['id', embedding_name]).filter("term", corpus=corpus)

    def persist(batch_docs, batch_units, type):
        batch_docs = ProcessedDocument.objects.bulk_create(batch_docs)
        batch_units_objs = []
        batch_size = 10000
        for doc, embs in zip(batch_docs, batch_units):
            ind = 0
            for emb in embs:
                batch_units_objs.append(AnalysisUnit(type=type,
                                                     processed_document=doc,
                                                     value=emb[by_unit],
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
        elif type_unit_int in [3, 4]:
            for elem in embeddings:
                document_embeddings.append(
                    {
                        by_unit: elem[by_unit],
                        "values": elem.layers[0].values
                    }
                )
        elif type_unit_int in [5]:
            document_embeddings.append(
                {
                    by_unit: embeddings[by_unit],
                    "values": embeddings.layers[0].values
                }
            )
        else:
            raise Exception("Unknown Unit_by type")
        batch_units.append(document_embeddings)
        if len(batch_docs) >= batch_size:
            persist(batch_docs, batch_units, type_unit_int)
            batch_docs = []
            batch_units = []
    persist(batch_docs, batch_units, type=type_unit_int)


def pool_vectors(cur_embed, pooling):
    import numpy as np

    data = np.array(cur_embed)
    if pooling == "Average":
        return list(np.average(data, axis=0))
    elif pooling == "Max":
        return list(np.max(data, axis=0))
    elif pooling == "Average+Max":
        return list(np.average(data, axis=0)) + list(np.max(data, axis=0))
    raise Exception("Unknown pooling")


def pool_word_to_sentence(document, embeddings_to_write, documents_to_write, from_embedding_name, from_embedding_by_unit, to_embedding_by_unit, pooling):
    embeddings = document[from_embedding_name]
    for sent in embeddings:
        cur_sent = ""
        cur_embed = []

        for token in sent[1:-1]:
            token_str = token[from_embedding_by_unit]
            token_emb = token['layers'][0]['values']

            cur_embed.append(token_emb)
            cur_sent += token_str.replace("##", "") + " "

        cur_embed = pool_vectors(cur_embed, pooling)
        embeddings_to_write[-1].append(
            {
                "layers": [
                    {
                        "values": cur_embed,
                        "index": -2
                    }
                ],
                to_embedding_by_unit: cur_sent
            }
        )
    documents_to_write.append(document)


def pool_sentence_to_text(document, embeddings_to_write, documents_to_write, from_embedding_name, from_embedding_by_unit, to_embedding_by_unit, pooling):
    embeddings = document[from_embedding_name]
    cur_text = ""
    cur_embed = []
    for sent in embeddings:
        token_str = sent[from_embedding_by_unit]
        token_emb = sent['layers'][0]['values']

        cur_embed.append(token_emb)
        cur_text += token_str.replace("##", "") + " "

    cur_embed = pool_vectors(cur_embed, pooling)
    embeddings_to_write[-1] = \
        {
            "layers": [
                {
                    "values": cur_embed,
                    "index": -2
                }
            ],
            to_embedding_by_unit: cur_text
        }
    documents_to_write.append(document)


def pool_document(document, embeddings_to_write, documents_to_write, from_embedding_name, to_embedding_by_unit, from_embedding_by_unit, pooling):
    if from_embedding_by_unit.lower() == "word":
        pool_word_to_sentence(document, embeddings_to_write, documents_to_write, from_embedding_name, from_embedding_by_unit, to_embedding_by_unit, pooling)
    elif from_embedding_by_unit.lower() == "sentence":
        pool_sentence_to_text(document, embeddings_to_write, documents_to_write, from_embedding_name, from_embedding_by_unit, to_embedding_by_unit, pooling)


def pool_embeddings(**kwargs):
    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT, ES_INDEX_EMBEDDING

    start = kwargs['start']
    end = kwargs['end']
    corpus = kwargs['corpus']
    from_embedding_name = kwargs['from_embedding_name']
    from_embedding_by_unit = kwargs['from_embedding_by_unit']
    to_embedding_name = kwargs['to_embedding_name']
    to_embedding_by_unit = kwargs['to_embedding_by_unit']
    pooling = kwargs['pooling']

    # Get embedding object
    query = {
        "corpus": corpus.lower(),
        # "is_ready": False, # TODO Uncomment
        "name": to_embedding_name.lower(),
    }
    embedding = search(ES_CLIENT, ES_INDEX_EMBEDDING, query)[-1]
    number_of_documents = embedding['number_of_documents']

    # Get documents
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT,
                       {"corpus": corpus.lower()},
                       start=int(start / 100 * number_of_documents),
                       end=int(end / 100 * number_of_documents),
                       source=['id', from_embedding_name],
                       sort=['id']
                       )

    embeddings_to_write = []
    documents_to_write = []
    batch_size = 10000
    # Pooling
    for document in documents:
        embeddings_to_write.append([])
        pool_document(document, embeddings_to_write, documents_to_write, from_embedding_name, to_embedding_by_unit, from_embedding_by_unit, pooling)
        # Update to ES
        if len(embeddings_to_write) >= batch_size:
            persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings_to_write, to_embedding_name)
            embeddings_to_write = []
            documents_to_write = []
    persist_embeddings_to_es(ES_CLIENT, ES_INDEX_DOCUMENT, documents_to_write, embeddings_to_write, to_embedding_name)
