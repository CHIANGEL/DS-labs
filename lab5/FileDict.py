import json
import os
from random import randrange
import datetime
from pprint import pprint

file_dict_dir = "./KV_data"

class FileDictionary:
    def __init__(self, GroupId, ServerId):
        self.GroupId = GroupId
        self.ServerId = ServerId
        if not os.path.exists(file_dict_dir):
            os.makedirs(file_dict_dir)
        self.filename = "{}-{}.json".format(self.GroupId, self.ServerId)
        self.filepath = os.path.join(file_dict_dir, self.filename)
        self.data = self.load() if os.path.isfile(self.filepath) else {}

    def get(self, key):
        return self.data[key]

    def put(self, key, value):
        self.data[key] = value

    def delete(self, key):
        del self.data[key]

    def load(self):
        print("FILE_DICT: Retrieving data from disk")
        with open(self.filepath, "r") as f:
            json_str = json.load(f)
            data = json.loads(json_str)
        return data

    def dump(self):
        with open(self.filepath, "w") as f:
            json_str = json.dumps(self.data)
            json.dump(json_str, f)

if __name__ == "__main__":
    num_records = 10

    # generate a random list of timestamps
    def random_date(start, l):
        current = start
        while l >= 0:
            curr = current + datetime.timedelta(minutes=randrange(60))
            yield curr
            l -= 1
    startDate = datetime.datetime(2018, 2, 10, 13, 00)
    timestamp_list = random_date(startDate, num_records)

    # Test
    fileDict = FileDictionary(1)
    for key, timestamp in zip(range(num_records), timestamp_list):
        item = {}
        item['key'] = key
        item['value'] = key+1
        item['timeStamp'] = timestamp.strftime("%m/%d/%y %H:%M")
        item['serverId'] = 1
        fileDict.put(item)
    print(fileDict.get(0))
    fileDict.dump()
    data = fileDict.load()
    pprint(data)
