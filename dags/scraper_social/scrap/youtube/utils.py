def parse_yt_comments(comments_dict_list):
    """
    comment_text, comment_date, comment_id
    :param comments_dict_list:
    :return:
    """
    from operator import itemgetter
    result = list()
    keys = ['text', 'comment_publish_date', 'comment_id']
    for comment in comments_dict_list:
        comment_meta = list(itemgetter(*keys)(comment))
        result.append(comment_meta)
    return result
