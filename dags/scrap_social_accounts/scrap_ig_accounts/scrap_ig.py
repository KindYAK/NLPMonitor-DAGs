def create_ig_accounts(**kwargs):
    from util.igramscraper.instagram import Instagram
    from scraping.models import SocialNetworkAccount, InstagramLoginPass
    from operator import itemgetter
    from time import sleep

    username = kwargs.get('username', 'tokayev_online')
    num_followers = kwargs.get('num_followers', 10_000)

    ig_account = InstagramLoginPass.objects.filter(is_active=True).first()

    instagram = Instagram()
    instagram.with_credentials(ig_account.login, ig_account.password)
    instagram.login(force=False, two_step_verificator=True)
    # TODO instagram.set_proxies(proxies)

    sleep(2)  # Delay to mimic user

    account = instagram.get_account(username)
    sleep(1)
    # Get num_followers followers of username, 100 a time with random delay between requests
    followers = instagram.get_followers(account.identifier, num_followers, 100, delayed=True)

    followers_list = list()
    for follower in followers['accounts']:
        if not follower.is_private:
            account_meta = itemgetter(*['identifier', 'username', 'full_name', 'is_private'])(follower.__dict__)
            object_dict = dict(zip(['account_id', 'nickname', 'name', 'is_private'], account_meta))
            object_dict['social_network'] = 3
            object_dict['url'] = f'https://www.instagram.com/{object_dict["nickname"]}/'
            sn_object = SocialNetworkAccount(**object_dict)
            followers_list.append(sn_object)

    SocialNetworkAccount.objects.bulk_create(followers_list, ignore_conflicts=True)
    return f'{len(followers_list)} not private followers processed, from {num_followers} requested'
