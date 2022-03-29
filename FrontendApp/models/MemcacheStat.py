class MemcacheStat:
    def __init__(self, data):
        mappings = [
            'num_items',
            'total_size',
            'num_requests',
            'mis_rate',
            'hit_rate']
        self.data = {}
        # for i in range(len(mappings)):
        #     self.data[mappings[i]] = []
        # for i in range(len(data)):
        #     for j in range(len(mappings)):
        #         self.data[mappings[j]].append(data[i][j])
        self.data = data

    def to_json(self):
        return self.data