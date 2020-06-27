import FileDict

GET_ERROR = -1
PUT_ERROR = -1
PUT_SUCCESS = 0
DELETE_ERROR = -1
DELETE_SUCCCESS = 0

class Model:
    def __init__(self, GroupId, ServerId):
        self.file_dict = FileDict.FileDictionary(GroupId, ServerId)
    
    def get(self, key):
        return self.file_dict.get(key)
    
    def put(self, key, value):
        self.file_dict.put(key, value)

    def delete(self, key):
        self.file_dict.delete(key)

    def dump(self):
        self.file_dict.dump()