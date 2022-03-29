import hashlib

import db.db_access

class HashRouter:

    def __init__(self, caches_url=None):
        if caches_url is None:
            caches_url = []
        self.caches_url = caches_url

    def get_cache_node_url(self, key):
        if len(self.caches_url) == 0:
            print('No caches available, waiting for resize signal.')
            return ''
        md5_hex = hashlib.md5(key.encode('utf-8')).hexdigest()
        cache_index = int(md5_hex, 16) % len(self.caches_url)
        res = self.caches_url[cache_index]
        print(f'Send to cache at : {res}')
        return res

    def set_caches(self, new_caches):
        self.caches_url = new_caches

    def get_all_cache_node_urls(self):
        return self.caches_url

    def init(self):
        fetch_result = db.db_access.get_all_avail_cache_instances_url()
        caches_url = []
        for res in fetch_result:
            caches_url.append(res[0])
        self.set_caches(caches_url)
        print(caches_url)

