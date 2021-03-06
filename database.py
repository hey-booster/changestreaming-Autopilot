import pymongo
import os


class db(object):
    user = os.environ.get('DB_USER')
    name = os.environ.get('DB_NAME')
    pw = os.environ.get('DB_PASSWORD')
    URI = "mongodb://%s:%s@heybooster-shard-00-00-yue91.mongodb.net:27017,heybooster-shard-00-01-yue91.mongodb.net:27017,heybooster-shard-00-02-yue91.mongodb.net:27017/test?ssl=true&replicaSet=heybooster-shard-0&authSource=admin&retryWrites=true&w=majority" % (
        user, pw)

    @staticmethod
    def init():
        client = pymongo.MongoClient(db.URI)
        db.DATABASE = client[db.name]


    @staticmethod
    def insert(collection, data):
        db.DATABASE[collection].insert(data)
        
    
    @staticmethod
    def insert_one(collection, data):
        return db.DATABASE[collection].insert_one(data)


    @staticmethod
    def find_one(collection, query):
        return db.DATABASE[collection].find_one(query)


    @staticmethod
    def find(collection, query):
        return db.DATABASE[collection].find(query)


    @staticmethod
    def find_and_modify(collection, query, **kwargs):
        print(kwargs)
        db.DATABASE[collection].find_and_modify(query=query,
                                                update={"$set": kwargs}, upsert=False,
                                                full_response=True)


class db2(object):
    user = os.environ.get('DB_USER')
    name = os.environ.get('DB_NAME')
    pw = os.environ.get('DB_PASSWORD')
    URI = "mongodb://%s:%s@heybooster-shard-00-00-yue91.mongodb.net:27017,heybooster-shard-00-01-yue91.mongodb.net:27017,heybooster-shard-00-02-yue91.mongodb.net:27017/test?ssl=true&replicaSet=heybooster-shard-0&authSource=admin&retryWrites=true&w=majority" % (
        user, pw)


    @staticmethod
    def init():
        client = pymongo.MongoClient(db2.URI)
        db2.DATABASE = client[db.name]


    @staticmethod
    def insert(collection, data):
        db2.DATABASE[collection].insert(data)
        
        
    @staticmethod
    def insert_one(collection, data):
        return db2.DATABASE[collection].insert_one(data)


    @staticmethod
    def find_one(collection, query):
        return db2.DATABASE[collection].find_one(query)


    @staticmethod
    def find(collection, query):
        return db2.DATABASE[collection].find(query)


    @staticmethod
    def find_and_modify(collection, query, **kwargs):
        print(kwargs)
        db2.DATABASE[collection].find_and_modify(query=query,
                                                update={"$set": kwargs}, upsert=False,
                                                full_response=True)