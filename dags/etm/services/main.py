def etm_calc(**kwargs):
    """
    :param kwargs:
    :return:
    """
    import torch
    from torch import optim
    import numpy as np
    import os

    from .data import get_data
    from .model import ETM
    from .utils import train_model, visualize, evaluate
    from util.constants import BASE_DAG_DIR

    seed = kwargs.get('seed', 666)
    corpus = kwargs.get('corpus', 'main')
    batch_size = kwargs.get('batch_size', 1000)
    num_topics = kwargs.get('num_topics', 100)
    optimizer = kwargs.get('optimizer', 'adam')
    t_hidden_size = kwargs.get('t_hidden_size', 800)  # dimension of hidden space of q(theta)
    save_path = kwargs.get('save_path', 'etm_models')  # path to save results
    emb_path = kwargs.get('emb_path', 'etm_embeddings')  # path to directory and file containing word embeddings

    rho_size = kwargs.get('rho_size', 300)  # dimension of rho
    emb_size = kwargs.get('emb_size', 300)  # dimension of embeddings
    theta_act = kwargs.get('theta_act', 'relu')  # activations tanh, softplus, relu, rrelu, leakyrelu, elu, selu, glu)'
    train_embeddings = kwargs.get('train_embeddings', 0)  # flag whether to fix rho or train it

    lr = kwargs.get('lr', 0.005)  # learning rate
    lr_factor = kwargs.get('lr_factor', 4.0)  # divide learning rate by this
    epochs = kwargs.get('epochs', 100)  # epochs to train
    enc_drop = kwargs.get('enc_drop', 0)  # dropout rate on encoder
    clip = kwargs.get('clip', 0)  # gradient clipping
    nonmono = kwargs.get('nonmono', 10)  # number of bad hits allowed
    wdecay = kwargs.get('wdecay', 1.2e-6)  # l2 regularization
    anneal_lr = kwargs.get('anneal_lr', 0)  # whether to anneal the learning rate or not

    num_words = kwargs.get('num_words', 10)  # number of words for topics visualization
    visualize_every = kwargs.get('visualize_every', 10)  # print topics every n epochs
    save_path = os.path.join(BASE_DAG_DIR, save_path)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    kwargs['device'] = device
    print('\n')
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)

    # get data
    # 1. vocabulary
    data_path = os.path.join(BASE_DAG_DIR, 'etm_temp')
    vocab, train, test = get_data(data_path)
    vocab_size = len(vocab)

    # 1. training data
    train_tokens = train['tokens']
    train_counts = train['counts']
    num_docs_train = len(train_tokens)
    kwargs['train_tokens'] = train_tokens
    kwargs['train_counts'] = train_counts
    kwargs['num_docs_train'] = num_docs_train

    # 3. test data
    test_tokens = test['tokens']
    test_counts = test['counts']
    num_docs_test = len(test_tokens)
    kwargs['test_tokens'] = test_tokens
    kwargs['test_counts'] = test_counts
    kwargs['num_docs_test'] = num_docs_test

    embeddings = None
    if not train_embeddings:
        emb_path = emb_path
        vectors = {}
        with open(emb_path, 'rb') as f:
            for l in f:
                line = l.decode().split()
                word = line[0]
                if word in vocab:
                    vect = np.array(line[1:]).astype(np.float)
                    vectors[word] = vect
        embeddings = np.zeros((vocab_size, emb_size))
        words_found = 0
        for i, word in enumerate(vocab):
            try:
                embeddings[i] = vectors[word]
                words_found += 1
            except KeyError:
                embeddings[i] = np.random.normal(scale=0.6, size=(emb_size,))
        embeddings = torch.from_numpy(embeddings).to(device)
        embeddings_dim = embeddings.size()

    print('=*' * 100)
    print('Training an Embedded Topic Model on {}'.format(corpus.upper()))
    print('=*' * 100)

    # define checkpoint
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    ckpt = os.path.join(save_path,
                        'etm_{}_K_{}_Htheta_{}_Optim_{}_Clip_{}_ThetaAct_{}_Lr_{}_Bsz_{}_RhoSize_{}_trainEmbeddings_{}'.format(
                            corpus, num_topics, t_hidden_size, optimizer, clip,
                            theta_act, lr, batch_size, rho_size, train_embeddings))

    # define model and optimizer
    model = ETM(num_topics, vocab_size, t_hidden_size, rho_size, emb_size,
                theta_act, embeddings, train_embeddings, enc_drop).to(device)

    print('model: {}'.format(model))

    if optimizer == 'adam':
        optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=wdecay)
    elif optimizer == 'adagrad':
        optimizer = optim.Adagrad(model.parameters(), lr=lr, weight_decay=wdecay)
    elif optimizer == 'adadelta':
        optimizer = optim.Adadelta(model.parameters(), lr=lr, weight_decay=wdecay)
    elif optimizer == 'rmsprop':
        optimizer = optim.RMSprop(model.parameters(), lr=lr, weight_decay=wdecay)
    elif optimizer == 'asgd':
        optimizer = optim.ASGD(model.parameters(), lr=lr, t0=0, lambd=0., weight_decay=wdecay)
    else:
        print('Defaulting to vanilla SGD')
        optimizer = optim.SGD(model.parameters(), lr=lr)

    # train model on data
    best_val_ppl = 1e9
    all_val_ppls = []
    print('\n')
    print('Visualizing model quality before training...')
    visualize(m=model, vocab=vocab, num_topics=num_topics, num_words=num_words, save_path=save_path)
    print('\n')
    for epoch in range(1, epochs):
        train_model(model=model, epoch=epoch, optimizer=optimizer)
        val_ppl = evaluate(m=model, source='train', vocab=vocab)
        if val_ppl < best_val_ppl:
            with open(ckpt, 'wb') as f:
                torch.save(model, f)
            best_val_ppl = val_ppl
        else:
            # check whether to anneal lr
            lr = optimizer.param_groups[0]['lr']
            if anneal_lr and (
                    len(all_val_ppls) > nonmono and val_ppl > min(all_val_ppls[:-nonmono]) and lr > 1e-5):
                optimizer.param_groups[0]['lr'] /= lr_factor
        if epoch % visualize_every == 0:
            visualize(m=model, vocab=vocab, num_topics=num_topics, num_words=num_words, save_path=save_path)
        all_val_ppls.append(val_ppl)
    with open(ckpt, 'rb') as f:
        model = torch.load(f)
    model = model.to(device)

    model.eval()

    with torch.no_grad():
        test_ppl, topic_coh, topic_div = evaluate(m=model, source='test', tc_td=True, vocab=vocab)
        print(f'!!! TEST PERPLEXITY {test_ppl}')
        # show topics
        beta = model.get_beta()
        print('\n')
        for k in range(num_topics):  # topic_indices:
            gamma = beta[k]
            top_words = list(gamma.cpu().numpy().argsort()[-num_words + 1:][::-1])
            topic_words = [vocab[a] for a in top_words]
            print('Topic {}: {}'.format(k, topic_words))

    return 666
