"""
  Collect financial indicators.
"""

from common.ts import ts_pro
from common.mongo import stock_mongo
import json
import time
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)


class FinIndCollector:
    def __init__(self):
        pass

    def get_all_fin_ind(self, start, end):
        """
        获取一个时间段内全部股票的财务报表指标并保存
        :param start: 开始时间
        :param end: 结束时间
        :return:
        """

        stocks = ts_pro.stock_basic()
        for idx, row in stocks.iterrows():
            try:
                ts_code = row['ts_code']
                df = ts_pro.fina_indicator(ts_code=ts_code, start_date=start, end_date=end)
                stock_mongo.insert_many('financial_indicator', json.loads(df.T.to_json()).values())
                result = 'succeed'
            except Exception as e:
                logging.error('Failed to get financial indicator of {}: {}'.format(row['ts_code'], e))
                result = 'failed'
            finally:
                logging.info('{}: {}'.format(row['ts_code'], result))
                time.sleep(1)

    def incremental_get_fi(self):
        """
        增量获取
        :return:
        """
        stocks = ts_pro.stock_basic()
        count = 0
        for idx, row in stocks.iterrows():
            try:
                ts_code = row['ts_code']
                rs = stock_mongo.stockdb.get_collection('financial_indicator').find({"ts_code": ts_code}).sort("end_date", -1).limit(1)
                if rs:
                    if rs[0]['end_date'] != '20200331' and rs[0]['end_date'] != '20191231':
                        count += 1
            except Exception as e:
                logging.error(e)
        print(count)

if __name__ == '__main__':
    fic = FinIndCollector()
    fic.get_all_fin_ind(start='20120101', end='20201231')
    # fic.incremental_get_fi()

