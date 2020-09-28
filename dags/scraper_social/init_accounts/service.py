def init_accounts(**kwargs):
    from airflow.models import Variable
    from scraping.models import SocialNetworkAccount
    import json

    ss = SocialNetworkAccount.objects.filter(is_active=True)
    Variable.set("social_accounts",
                     json.dumps(
                         [
                             {
                                 "id": s.id,
                                 "url": s.url,
                                 "account_id": s.account_id,
                                 "priority_rate": s.priority_rate,
                                 # Get datetime back with datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
                                 "datetime_last_parsed": s.datetime_last_parsed.isoformat()[:-6] if s.datetime_last_parsed else None,
                                 "social_network": s.social_network,
                             } for s in ss
                         ]
                     )
                 )

    Variable.set("social_networks",
                     json.dumps(
                         [
                             {
                                 "id": s[0],
                                 "name": s[1],
                             } for s in SocialNetworkAccount.SOCIAL_NETWORKS
                         ]
                     )
                 )
