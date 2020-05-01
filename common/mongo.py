import pymongo


class StockMongo:
    def __init__(self, host='localhost', port=27017, dbname='stock'):
        self.client = pymongo.MongoClient(host, port)
        self.stockdb = self.client[dbname]

    def insert_many(self, collection, docs):
        """
        :param collection: collection name
        :param docs: documents to be inserted
        :return:
        """
        self.stockdb.get_collection(collection).insert_many(documents=docs)

    def insert_one(self, collection, doc):
        """

        :param collection: collection name
        :param doc: doc to be inserted
        :return:
        """
        self.stockdb.get_collection(collection).insert_one(document=doc)

    def count(self, collection, filter):
        """
        Count the documents
        :param collection:
        :param criteria:
        :return:
        """
        return self.stockdb.get_collection(collection).count(filter=filter)

    def find(self, collection, *args, **kwargs):
        """
        Find the documents
        :param collection:
        :param kwargs:
        :return:
        """
        return self.stockdb.get_collection(collection).find(*args, **kwargs)


stock_mongo = StockMongo(host='192.168.3.109', port=27017, dbname='stock_data')

