import datetime


def es_filter_term(search, key, value):
    query = 'term'
    return search.filter(query, **{key: value})


def search(client, index, query, start=None, end=None, source=None, sort=None, get_scan_obj=False, get_search_obj=False):
    from elasticsearch_dsl import Search
    s = Search(using=client, index=index)
    for key, value in query.items():
        if any(key.endswith(range_selector) for range_selector in ['__gte', '__lte', '__gt', '__lt']):
            range_selector = key.split("__")[-1]
            s = s.filter('range', **{key.replace(f"__{range_selector}", ""): {range_selector: value}})
        else:
            s = es_filter_term(s, key, value)
    if source:
        s = s.source(include=source)
    if sort:
        s = s.sort(*sort)
    s = s[start:end]
    if get_scan_obj:
        return s.sort()
    elif get_search_obj:
        return s
    else:
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
    return 0


def update_generator(index, documents, body=None):
    for document in documents:
        yield {
            "_index": index,
            "_op_type": "update",
            "_id": document.meta.id,
            "doc": body if body else document.to_dict(),
        }


def get_count(client, index):
    from elasticsearch_dsl import Search
    return Search(using=client, index=index).count()
