import datetime


def es_filter_term(search, key, value):
    query = 'term'
    return search.filter(query, **{key: value})


def search(client, index, query, start=None, end=None):
    from elasticsearch_dsl import Search
    s = Search(using=client, index=index)
    for key, value in query.items():
        s = es_filter_term(s, key, value)

    s = s[start:end]
    return s.execute()


def update_instance(client, index, params):
    _id = params.pop('_id')
    params.pop('_type')
    params['modified'] = datetime.datetime.now()
    data = {
        'doc': params
    }
    client.update(
        index=index,
        id=_id,
        body=data
    )
