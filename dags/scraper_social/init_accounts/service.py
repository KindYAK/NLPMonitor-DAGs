def init_accounts(**kwargs):
    from airflow.models import Variable
    from scraping.models import SocialNetworkAccount, MonitoringQuery
    import json

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

    mqs = MonitoringQuery.objects.filter(is_active=True)
    Variable.set("monitoring_queries",
                     json.dumps(
                         [
                             {
                                 "id": mq.id,
                                 "query": mq.query,
                                 "priority_rate": mq.priority_rate,
                                 # Get datetime back with datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
                                 "datetime_last_parsed": mq.datetime_last_parsed.isoformat()[:-6] if mq.datetime_last_parsed else None,
                                 "social_network": mq.social_network,
                             } for mq in mqs
                         ]
                     )
                 )
