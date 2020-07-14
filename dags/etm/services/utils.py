import numpy as np


def get_topic_diversity(beta, topk):
    num_topics = beta.shape[0]
    list_w = np.zeros((num_topics, topk))
    for k in range(num_topics):
        idx = beta[k, :].argsort()[-topk:][::-1]
        list_w[k, :] = idx
    n_unique = len(np.unique(list_w))
    TD = n_unique / (topk * num_topics)
    print('Topic diveristy is: {}'.format(TD))


def get_document_frequency(data, wi, wj=None):
    if wj is None:
        D_wi = 0
        for l in range(len(data)):
            doc = data[l].squeeze(0)
            if len(doc) == 1:
                continue
            else:
                doc = doc.squeeze()
            if wi in doc:
                D_wi += 1
        return D_wi
    D_wj = 0
    D_wi_wj = 0
    for l in range(len(data)):
        doc = data[l].squeeze(0)
        if len(doc) == 1:
            doc = [doc.squeeze()]
        else:
            doc = doc.squeeze()
        if wj in doc:
            D_wj += 1
            if wi in doc:
                D_wi_wj += 1
    return D_wj, D_wi_wj


def get_topic_coherence(beta, data, vocab):
    D = len(data)  # number of docs...data is list of documents
    print('D: ', D)
    TC = []
    num_topics = len(beta)
    for k in range(num_topics):
        print('k: {}/{}'.format(k, num_topics))
        top_10 = list(beta[k].argsort()[-11:][::-1])
        top_words = [vocab[a] for a in top_10]
        TC_k = 0
        counter = 0
        for i, word in enumerate(top_10):
            # get D(w_i)
            D_wi = get_document_frequency(data, word)
            j = i + 1
            tmp = 0
            while j < len(top_10) and j > i:
                # get D(w_j) and D(w_i, w_j)
                D_wj, D_wi_wj = get_document_frequency(data, word, top_10[j])
                # get f(w_i, w_j)
                if D_wi_wj == 0:
                    f_wi_wj = -1
                else:
                    f_wi_wj = -1 + (np.log(D_wi) + np.log(D_wj) - 2.0 * np.log(D)) / (np.log(D_wi_wj) - np.log(D))
                # update tmp:
                tmp += f_wi_wj
                j += 1
                counter += 1
            # update TC_k
            TC_k += tmp
        TC.append(TC_k)
    print('counter: ', counter)
    print('num topics: ', len(TC))
    TC = np.mean(TC) / counter
    print('Topic coherence is: {}'.format(TC))


def train_model(**kwargs):
    """

    :param kwargs:
    :return:
    """
    import torch
    from .data import get_batch

    model = kwargs.get('model')
    epoch = kwargs.get('epoch')
    optimizer = kwargs.get('optimizer')
    bow_norm = kwargs.get('bow_norm')
    batch_size = kwargs.get('batch_size')
    clip = kwargs.get('clip')
    device = kwargs.get('device')
    vocab_size = kwargs.get('vocab_size')
    log_interval = kwargs.get('log_interval')
    train_tokens = kwargs.get('train_tokens')
    train_counts = kwargs.get('train_counts')
    num_docs_train = kwargs.get('num_docs_train')

    model.train()
    acc_loss = 0
    acc_kl_theta_loss = 0
    cnt = 0
    indices = torch.randperm(num_docs_train)
    indices = torch.split(indices, batch_size)
    for idx, ind in enumerate(indices):
        optimizer.zero_grad()
        model.zero_grad()
        data_batch = get_batch(train_tokens, train_counts, ind, vocab_size, device)
        sums = data_batch.sum(1).unsqueeze(1)
        if bow_norm:
            normalized_data_batch = data_batch / sums
        else:
            normalized_data_batch = data_batch
        recon_loss, kld_theta = model(data_batch, normalized_data_batch)
        total_loss = recon_loss + kld_theta
        total_loss.backward()

        if clip > 0:
            torch.nn.utils.clip_grad_norm_(model.parameters(), clip)
        optimizer.step()

        acc_loss += torch.sum(recon_loss).item()
        acc_kl_theta_loss += torch.sum(kld_theta).item()
        cnt += 1

        if idx % log_interval == 0 and idx > 0:
            cur_loss = round(acc_loss / cnt, 2)
            cur_kl_theta = round(acc_kl_theta_loss / cnt, 2)
            cur_real_loss = round(cur_loss + cur_kl_theta, 2)

            print('Epoch: {} .. batch: {}/{} .. LR: {} .. KL_theta: {} .. Rec_loss: {} .. NELBO: {}'.format(
                epoch, idx, len(indices), optimizer.param_groups[0]['lr'], cur_kl_theta, cur_loss, cur_real_loss))

    cur_loss = round(acc_loss / cnt, 2)
    cur_kl_theta = round(acc_kl_theta_loss / cnt, 2)
    cur_real_loss = round(cur_loss + cur_kl_theta, 2)
    print('*' * 100)
    print('Epoch----->{} .. LR: {} .. KL_theta: {} .. Rec_loss: {} .. NELBO: {}'.format(
        epoch, optimizer.param_groups[0]['lr'], cur_kl_theta, cur_loss, cur_real_loss))
    print('*' * 100)


def evaluate(m, source, vocab, tc=False, td=False):
    """
    Compute perplexity on document completion.
    """
    import torch
    import math
    from .data import get_batch
    m.eval()
    with torch.no_grad():

        # get \beta here
        beta = m.get_beta()

        # do dc and tc here
        acc_loss = 0
        cnt = 0
        indices_1 = torch.split(torch.tensor(range(args.num_docs_test_1)), args.eval_batch_size)
        for idx, ind in enumerate(indices_1):
            # get theta from first half of docs
            data_batch_1 = get_batch(test_1_tokens, test_1_counts, ind, args.vocab_size, device)
            sums_1 = data_batch_1.sum(1).unsqueeze(1)
            if args.bow_norm:
                normalized_data_batch_1 = data_batch_1 / sums_1
            else:
                normalized_data_batch_1 = data_batch_1
            theta, _ = m.get_theta(normalized_data_batch_1)

            # get prediction loss using second half
            data_batch_2 = get_batch(test_2_tokens, test_2_counts, ind, args.vocab_size, device)
            sums_2 = data_batch_2.sum(1).unsqueeze(1)
            res = torch.mm(theta, beta)
            preds = torch.log(res)
            recon_loss = -(preds * data_batch_2).sum(1)

            loss = recon_loss / sums_2.squeeze()
            loss = loss.mean().item()
            acc_loss += loss
            cnt += 1
        cur_loss = acc_loss / cnt
        ppl_dc = round(math.exp(cur_loss), 1)
        print('*' * 100)
        print('{} Doc Completion PPL: {}'.format(source.upper(), ppl_dc))
        print('*' * 100)
        if tc or td:
            beta = beta.cpu().numpy()
            if tc:
                print('Computing topic coherence...')
                get_topic_coherence(beta, train_tokens, vocab)
            if td:
                print('Computing topic diversity...')
                get_topic_diversity(beta, 25)
        return ppl_dc


def visualize(m, vocab, num_topics, num_words):
    """

    :param m:
    :param vocab:
    :param num_topics:
    :param num_words:
    :return:
    """
    import os
    import torch
    if not os.path.exists('./results'):
        os.makedirs('./results')

    m.eval()

    # visualize topics using monte carlo
    with torch.no_grad():
        print('#' * 100)
        print('Visualize topics...')
        topics_words = []
        gammas = m.get_beta()
        for k in range(num_topics):
            gamma = gammas[k]
            top_words = list(gamma.cpu().numpy().argsort()[-num_words + 1:][::-1])
            topic_words = [vocab[a] for a in top_words]
            topics_words.append(' '.join(topic_words))
            print('Topic {}: {}'.format(k, topic_words))


def split_bow(bow_in, n_docs):
    indices = [[w for w in bow_in[doc, :].indices] for doc in range(n_docs)]
    counts = [[c for c in bow_in[doc, :].data] for doc in range(n_docs)]
    return indices, counts


def create_bow(doc_indices, words, n_docs, vocab_size):
    from scipy import sparse
    return sparse.coo_matrix(([1] * len(doc_indices), (doc_indices, words)), shape=(n_docs, vocab_size)).tocsr()


def create_list_words(in_docs):
    return [x for y in in_docs for x in y]


def create_doc_indices(in_docs):
    aux = [[j for i in range(len(doc))] for j, doc in enumerate(in_docs)]
    return [int(x) for y in aux for x in y]


def remove_empty(in_docs):
    return [doc for doc in in_docs if doc != []]

