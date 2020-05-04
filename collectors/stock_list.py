"""
  Collect stock list and store into MongoDB
"""

from common.g import ts_pro
from common.g import stockdb
from common import cons
import json


class StockListCollector:
    def __init__(self):
        pass

    def refresh_stock_list(self):
        stocks = ts_pro.stock_basic()
        if not stocks.empty:
            stockdb.get_collection(cons.S_STOCK_LIST).drop()
            stockdb.get_collection(cons.S_STOCK_LIST).insert_many(json.loads(stocks.T.to_json()).values())


if __name__ == '__main__':
    slc = StockListCollector()
    slc.refresh_stock_list()
