# -*- coding: utf-8 -*-

import pymongo

_SERVER = '34.208.174.174:27017'
_USER = 'dbc'
_PASSWORD = 'Ipg!@3dbc'
_AUTH_SOURCE = 'dbc'

_DB_NAME = 'dbc'
_COLLECTION_NAME = 'es_cleanup'

_CLIENT = pymongo.MongoClient(_SERVER,
                              username=_USER,
                              password=_PASSWORD,
                              authSource=_AUTH_SOURCE,
                              authMechanism='SCRAM-SHA-256')
_DB = _CLIENT[_DB_NAME]
COLLECTION = _DB[_COLLECTION_NAME]

# create an index to avoid inserting duplicated records
COLLECTION.create_index([("id", pymongo.ASCENDING)], unique=True)


def save2db(data):
    try:
        x = COLLECTION.insert_many(data, ordered=False)
    except:
        pass
    pass


def truncate_collection():
    x = COLLECTION.delete_many({})
    return x.deleted_count






