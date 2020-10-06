def get_comments(post: dict) -> list:
    from datetime import datetime
    """
    :param post:
    :return List:
    """
    comments_list = list()
    for comment in post['edge_media_to_parent_comment']['edges']:
        node_comments = comment['node']
        comments_text = node_comments['text']
        comments_date = datetime.fromtimestamp(node_comments['created_at'])
        comments_id = node_comments['id']
        comments_list.append([comments_text, comments_date, comments_id])
        if node_comments['edge_threaded_comments']['count']:
            for sub_comment in node_comments['edge_threaded_comments']['edges']:
                sub_comment_node = sub_comment['node']
                sub_comment_date = datetime.fromtimestamp(sub_comment_node['created_at'])
                sub_comments_id = sub_comment_node['id']
                comments_list.append([sub_comment_node['text'], sub_comment_date, sub_comments_id])
    return comments_list


def get_bio(account_dict):
    """
    :param account_dict:
    :return:
    """
    account_info = dict()
    account_info['biography'] = account_dict['biography']
    account_info['link'] = account_dict['external_url']
    account_info['fullname'] = account_dict['full_name']
    account_info['followers_num'] = account_dict['edge_followed_by']['count']
    account_info['followees_num'] = account_dict['edge_follow']['count']
    account_info['is_private'] = account_dict['is_private']
    return account_info


def get_posts(method_type, object_id, num_posts=100):
    """
    :param method_type:
    :param object_id:
    :param num_posts:
    :return:
    """
    from itertools import cycle
    from random import shuffle
    from util.instagram import WebAgent, Location, Account
    import os
    from util.constants import BASE_DAG_DIR

    with open(os.path.join(BASE_DAG_DIR, "proxy_list.txt"), "r+") as f:
        proxies = f.read().split('\n\n')

    crashes_dict = dict(zip(proxies, [0] * len(proxies)))

    shuffle(proxies)
    infinite_proxie_loop = cycle(proxies)

    for proxie in infinite_proxie_loop:
        agent = WebAgent()
        settings = {'proxies': {'http': proxie}}

        try:
            if method_type in ['Account', 'Bio', 'Media_by_location']:
                if method_type in ['Account', 'Bio']:
                    buffer = Account(object_id)
                    if method_type == 'Bio':
                        account_dict = agent.update(buffer, settings=settings)
                        return get_bio(account_dict=account_dict)
                else:
                    buffer = Location(object_id)
                obj, _ = agent.get_media(buffer, count=num_posts, delay=0.01, settings=settings)

            elif method_type == 'Media':
                obj = agent.update(object_id, settings=settings)
                return get_comments(obj)
            else:
                location = Location(object_id)
                obj = agent.update(location, settings=settings)

            return obj

        except Exception as e:  # TODO detect specific error
            crashes_dict[proxie] += 1
            print(f'Proxie {proxie} crashed with error:')
            print(type(e))
            if crashes_dict[proxie] >= 5:
                print(f'Proxie {proxie} with 5 crashes was deleted')
                del crashes_dict[proxie]

        finally:
            if not len(crashes_dict):
                print('No legit proxies, break the loop')
                # TODO get new list of unique proxies and call get posts method inside
                break


def instagram_iterator(account, batch_size):
    medias_list = get_posts(method_type='Account', object_id=account.nickname, num_posts=batch_size)
    for media in medias_list:
        yield media
