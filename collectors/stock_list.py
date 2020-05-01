"""
  Collect stock list and store into MongoDB
"""

from common.ts import ts_pro
from common.mongo import stock_mongo
import json


class StockListCollector:
    def __init__(self):
        pass

    def get_stock_list(self):
        stocks = ts_pro.stock_basic()
        print(stocks)
        stock_mongo.insert_many('stock_list', json.loads(stocks.T.to_json()).values())


if __name__ == '__main__':
    slc = StockListCollector()
    slc.get_stock_list()
