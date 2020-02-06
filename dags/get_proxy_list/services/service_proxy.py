def get_proxy_list(**kwargs):
    import os
    import requests

    from util.constants import BASE_DAG_DIR

    key = "ODg4.XjuW9g.JqRqEoQ7lp9r7UqNnmkk3RQYrYI"
    timeout_limit = 3
    number_of_proxies = 50
    r = requests.get(
                        f'https://proxy11.com/api/proxy.json'
                        f'?key={key}'
                        f'&speed={timeout_limit}'
                        f'&limit={number_of_proxies}'
                        f'&type={"anonymous"}'
                     )
    proxylist = r.json()

    with open(os.path.join(BASE_DAG_DIR, "proxy_list.txt"), "w") as f:
        for proxy in proxylist['data']:
            f.write(f"http://{proxy['ip']}:{proxy['port']}\n")
