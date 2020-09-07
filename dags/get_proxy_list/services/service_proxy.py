def get_proxy_list():
    import asyncio
    import datetime
    import os

    from proxybroker import Broker

    from util.constants import BASE_DAG_DIR

    async def get_list(proxies, proxy_list):
        start_time = datetime.datetime.now()
        while True:
            proxy = await proxies.get()
            if (datetime.datetime.now() - start_time).seconds / 3600 >= 1:
                print("!!!", "Timeout!", datetime.datetime.now())
                break
            if proxy is None:
                break
            proxy_list.append(f"http://{proxy.host}:{proxy.port}\n")

    proxies = asyncio.Queue()
    broker = Broker(proxies)
    proxy_list = []
    tasks = asyncio.gather(
        broker.find(
            types=[
                ('HTTP', ('Anonymous', 'High')),
                ('HTTPS', ('Anonymous', 'High'))
            ],
            limit=100
        ),
        get_list(proxies, proxy_list)
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    with open(os.path.join(BASE_DAG_DIR, "proxy_list.txt"), "w") as f:
        for proxy in proxy_list:
            f.write(f"{proxy}\n")
