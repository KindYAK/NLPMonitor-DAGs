def scrap_wrapper(**kwargs):
    social_network_id = kwargs['social_network']
    result = None
    if social_network_id == 0:
        # Parse Facebook
        raise Exception("Not implemented")
    if social_network_id == 1:
        # Parse VK
        raise Exception("Not implemented")
    if social_network_id == 2:
        # Parse Twitter
        raise Exception("Not implemented")
    if social_network_id == 3:
        # Parse Instagram
        raise Exception("Not implemented")
    if social_network_id == 4:
        # Parse Telegram
        result = scrap_telegram(**kwargs)
    if social_network_id == 5:
        # Parse Youtube
        raise Exception("Not implemented")
    if results is None:
        raise Exception("Not implemented")
    else:
        return result


def scrap_telegram(**kwargs):
    from mainapp.models import Document, Author

    # Document.objects.create(**new)
    return f"Parse complete, {0} parsed"
