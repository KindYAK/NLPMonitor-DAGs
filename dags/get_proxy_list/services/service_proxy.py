def get_proxy_list():
    import asyncio
    import os

    from proxybroker import Broker

    from util.constants import BASE_DAG_DIR

    async def get_list(proxies, proxy_list):
        while True:
            proxy = await proxies.get()
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
            limit=250
        ),
        get_list(proxies, proxy_list)
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)

    with open(os.path.join(BASE_DAG_DIR, "proxy_list.txt"), "w") as f:
        for proxy in proxy_list:
            f.write(f"{proxy}\n")
