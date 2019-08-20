import pymongo
from collections import OrderedDict

# client = pymongo.MongoClient('34.208.174.174:27017',
#                              username='dbc',
#                              password='Ipg!@3dbc',
#                              authSource='dbc',
#                              authMechanism='SCRAM-SHA-256')
#
# db = client['dbc']
# collection = db['es_cleanup']
#
#
# x = collection.delete_many({})
# print(x.deleted_count)
#
# print(db)
# print(collection)
#
# collection.create_index([("id", pymongo.ASCENDING)], unique=True)
# print(collection.index_information())

#
# print(x)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient["local"]
collection = mydb['test']

# collection.insert_many([
#     {"id": 1, "value": 1, "value2": [1, 1]},
#     {"id": 2, "value": 2, "value2": [2, 2]},
#     {"id": 3, "value": 3, "value2": [3, 3]},
#     {"id": 4, "value": 4, "value2": [4, 4]},
#     {"id": 5, "value": 5, "value2": [5, 5]},
#     {"id": 6, "value": 6, "value2": [6, 6]},
# ])

data = [
    {"id": 1, "value": 10, "value2": [10, 10]},
    {"id": 3, "value": 30, "value2": [30, 30]},
    {"id": 5, "value": 50, "value2": [50, 50]},
]

collection.update_many(
    {
        "id": {
            "$in": list(map(lambda x: x['id'], data))
        }
    },
    {
        "$set": {
            "value2": ["$id", "$id"]
        }
    }
)
