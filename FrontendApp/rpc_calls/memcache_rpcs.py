from config.Config import DevConfig
from rpc_calls import send_requests
from extensions import hash_router


def call_put(key, value):
    server_url = hash_router.get_cache_node_url(key)
    if server_url == '':
        return None
    url = server_url + DevConfig.END_POINTS['put']
    data = {
        "key": key,
        "value": value
    }
    send_requests.post(url, data=data)
    return True


def call_get(key):
    server_url = hash_router.get_cache_node_url(key)
    if server_url == '':
        return None
    url = server_url + DevConfig.END_POINTS['get']
    data = {
        "key": key,
    }
    res = send_requests.post(url, data=data)
    if res is None:
        return None
    # Handle the response with timeout
    return res.json()


def call_clear():
    server_urls = hash_router.get_all_cache_node_urls()
    for url in server_urls:
        url = url + DevConfig.END_POINTS['clear']
        send_requests.post(url)
    return None


def call_invalidate_key(key):
    server_url = hash_router.get_cache_node_url(key)
    if server_url == '':
        return None
    url = server_url + DevConfig.END_POINTS['invalidatekey']
    data = {
        "key": key,
    }
    send_requests.post(url, data=data)
    return


# def call_refresh_configuration():
#     url = DevConfig.MEMCACHE_SERVER + DevConfig.END_POINTS['refreshconfiguration']
#     res = requests.post(url)
#     return res.json()
