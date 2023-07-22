from pymongo import MongoClient
from datetime import datetime

class MongoRouter:
    client = MongoClient("mongodb://localhost:27017")
    mongo_db = client['test']

    def disconnect(self):
        self.client.close()

    def all_collection(self):
        return self.mongo_db.list_collection_names()

    # create collection and add data
    def insert_to_collection(self, collection: str, data: list):
        # data = [{},{},{}] : list of data
        mycol = self.mongo_db[collection]
        mycol.insert_many(data)
        return data

    # find query or all datas in collention
    def find_spec(self, collection: str, data: dict = None, sort: dict = None, limit: int = None):
        # if data is None print all in collection
        mycol = self.mongo_db[collection]

        if data:
            if sort and limit:
                res = mycol.find(data).sort(sort).limit(limit)
            elif sort and limit is None:
                res = mycol.find(data).sort(sort)
            elif sort is None and limit:
                res = mycol.find(data).limit(limit)
            else:
                res = mycol.find(data)
        else:
            if sort and limit:
                res = mycol.find().sort(sort).limit(limit)
            elif sort and limit is None:
                res = mycol.find().sort(sort)
            elif sort is None and limit:
                res = mycol.find().limit(limit)
            else:
                res = mycol.find()

        return list(res)

    def delrup(self, collection: str, data: dict = None, drop=False, many=False):

        mycol = self.mongo_db[collection]
        if not drop:
            if many:
                mycol.delete_many(data)
                return data
            mycol.delete_one(data)
        elif drop:
            mycol.drop()

    def update(self, collection: str, old_data: dict, new_data: dict):
        mycol = self.mongo_db[collection]
        new_data = {"$push": new_data}
        mycol.update_one(old_data, new_data)

    def update1(self, collection: str, old_data: dict, new_data: dict):
        mycol = self.mongo_db[collection]
        mycol.update_one(old_data, new_data)

    def add_high_conditions(self, data):
        collection_name = "high_condition"
        data_list = [data]
        self.insert_to_collection(collection_name, data_list)

    def read_high_condition(self, mode, symbol):
        collection_name = "high_condition"
        data = {'mode': mode, f'data.symbol': symbol}
        result = self.find_spec(collection_name, data)
        for i in result:
            if i['data']['symbol'] == symbol:
                res = i['data']['data']
                return res[-1]
            else:
                return False

    def update_high_condition(self, mode, symbol, new_data):
        collection_name = "high_condition"
        old_data = {'mode': mode, f'data.symbol': symbol}
        new_data = {'data.data': new_data}
        self.update(collection_name, old_data, new_data)

    def add_high_condition_data(self, symbol, data):
        collection_name = "high_condition_data"
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dt = {'mode': 'stdev', 'data': {'symbol': symbol, 'date': date, 'data': data}}
        data_list = [dt]
        self.insert_to_collection(collection_name, data_list)

    def read_high_condition_data(self, mode, symbol):
        collection_name = "high_condition_data"
        data = {'mode': mode, f'data.symbol': symbol}
        result = self.find_spec(collection_name, data)
        for i in result:
            if i['data']['symbol'] == symbol:
                res = i['data']['data']
                return res
            else:
                return False

    def update_high_condition_data(self, mode, symbol, new_data):
        collection_name = "high_condition_data"
        old_data = {'mode': mode, f'data.symbol': symbol}
        del_array = {'$pop': {'data.data': 1}}
        add_array = {'$push': {'data.data': new_data}}
        self.update1(collection_name, old_data, del_array)
        self.update1(collection_name, old_data, add_array)
