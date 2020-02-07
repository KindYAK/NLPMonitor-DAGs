def get_proxy_list():
    import asyncio
    import os

    from proxybroker import Broker

    from util.constants import BASE_DAG_DIR

    async def write_to_file(proxies):
        with open(os.path.join(BASE_DAG_DIR, "proxy_list.txt"), "w") as f:
            while True:
                proxy = await proxies.get()
                if proxy is None:
                    break
                f.write(f"http://{proxy.host}:{proxy.port}\n")

    proxies = asyncio.Queue()
    broker = Broker(proxies)
    tasks = asyncio.gather(
        broker.find(
            types=[
                ('HTTP', ('Anonymous', 'High')),
                ('HTTPS', ('Anonymous', 'High'))
            ],
            limit=1000
        ),
        write_to_file(proxies)
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)
