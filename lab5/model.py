GET_ERROR = -1
PUT_ERROR = -1
PUT_SUCCESS = 0
DELETE_ERROR = -1
DELETE_SUCCCESS = 0

class Model:
    def __init__(self):
        self.KV_dist = {}
    
    def get(self, key):
        return self.KV_dist[key]
    
    def put(self, key, value):
        self.KV_dist[key] = value

    def delete(self, key):
        del self.KV_dist[key]