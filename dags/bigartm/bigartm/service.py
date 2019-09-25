def dataset_prepare(**kwargs):
    import os
    import artm

    from dags.bigartm.bigartm.cleaners import return_cleaned_array, txt_writer
    from util.constants import BASE_DAG_DIR
    from util.service_es import search, update_generator, get_count

    from nlpmonitor.settings import ES_CLIENT, ES_INDEX_DOCUMENT

    lc = artm.messages.ConfigureLoggingArgs()
    lib = artm.wrapper.LibArtm(logging_config=lc)
    lc.minloglevel = 3  # 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL
    lib.ArtmConfigureLogging(lc)

    name = kwargs['name']
    # Extract
    query = {
        "corpus": "main",
    }
    num_documents = get_count(ES_CLIENT, ES_INDEX_DOCUMENT)
    num_documents = 100 # TODO remove
    documents = search(ES_CLIENT, ES_INDEX_DOCUMENT, query, end=num_documents, source=["id", "text", "title", "source", "datetime"])
    ids = (d.id for d in documents)
    texts = return_cleaned_array((d.text for d in documents))
    titles = return_cleaned_array((d.title for d in documents))
    sources = return_cleaned_array((d.source for d in documents))
    dates = return_cleaned_array((d.datetime if hasattr(d, "datetime") else "" for d in documents))

    formated_data = []
    for id, text, title, source, date in zip(ids, texts, titles, sources, dates):
        formated_data.append(f'doc_{id}' + ' ' +
                                   '|text' + ' ' + text + ' ' +
                                   '|title' + ' ' + title + ' ' +
                                   '|source' + ' ' + source + ' ' +
                                   '|date' + ' ' + date)

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name}")
    if not os.path.exists(data_folder):
        os.mkdir(data_folder)
    txt_writer(data=formated_data, filename=os.path.join(data_folder, f"bigartm_formated_data.txt"))
    batch_vectorizer = artm.BatchVectorizer(data_path=os.path.join(data_folder, f"bigartm_formated_data.txt"),
                                            data_format="vowpal_wabbit",
                                            target_folder=os.path.join(data_folder, "batches"))
    return "Preprocessing complete"


def topic_modelling(**kwargs):
    import artm
    import os
    import numpy as np

    from util.constants import BASE_DAG_DIR

    name = kwargs['name']

    data_folder = os.path.join(BASE_DAG_DIR, "bigartm_temp")
    data_folder = os.path.join(data_folder, f"bigartm_formated_data_{name}")
    batch_vectorizer = artm.BatchVectorizer(data_path=os.path.join(data_folder, "batches"),
                                            data_format='batches')
    dictionary = artm.Dictionary()
    dictionary.gather(batch_vectorizer.data_path)

    model_artm = artm.ARTM(num_topics=3,
                           class_ids={"text": 1},
                           reuse_theta=True, cache_theta=True, seed=-1)
    model_artm.initialize(dictionary)
    # adding score to the model
    model_artm.scores.add(artm.PerplexityScore(name='PerplexityScore'))
    model_artm.scores.add(artm.TopicKernelScore(name='TopicKernelScore', class_id='text', probability_mass_threshold=0.3))

    # fit model
    model_artm.fit_offline(batch_vectorizer=batch_vectorizer, num_collection_passes=10)
    print(model_artm.get_theta())
    return "Topic Modelling complete"
