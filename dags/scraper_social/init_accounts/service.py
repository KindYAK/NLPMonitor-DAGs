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
                             } for s in ss
                         ]
                     )
                 )
