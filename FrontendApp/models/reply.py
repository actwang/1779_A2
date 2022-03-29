from models.error import Error

from models.MemcacheStat import MemcacheStat

class Reply:

    def __init__(self, success=True, error=Error(-1, "Not error message given"), keys=[], content=None, stat=None):
        self.success = success
        self.error = error
        self.keys = keys
        self.content = content
        self.memcache_stat = stat

    def to_json(self):
        res = {}
        if self.success:
            res["success"] = "true"
        else:
            res["success"] = "false"
            res["error"] = self.error.to_json()
            return res

        if len(self.keys) > 0:
            res["keys"] = self.keys
            return res

        if self.memcache_stat is not None:
            res["memcache_stat"] = MemcacheStat(self.memcache_stat).to_json()
            return res

        if self.content is not None:
            res["content"] = self.content
            return res
        return res
