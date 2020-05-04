"""
  Collect financial indicators.
"""

import json
import time
import logging
import sys
from common import cons
from common.g import ts_pro
from common.g import stockdb
import datetime

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
                stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).insert_many(json.loads(df.T.to_json()).values())
                result = 'succeed'
            except Exception as e:
                logging.error('Failed to get financial indicator of {}: {}'.format(row['ts_code'], e))
                result = 'failed'
            finally:
                logging.info('{}: {}'.format(row['ts_code'], result))
                # 限频
                time.sleep(1)

    def incremental_get_fi(self):
        """
        增量获取财务指标
        :return:
        """
        today = datetime.datetime.now().strftime('%Y%m%d')
        latest_fi = stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find().sort("end_date", -1).limit(1)
        latest_fi = list(latest_fi)
        if latest_fi:
            df = ts_pro.stock_basic()
            all_stocks = []
            for idx, row in df.iterrows():
                all_stocks.append(row['ts_code'])

            latest_end_date = latest_fi[0]['end_date']
            stocks_with_latest_fi = stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find({"end_date": latest_end_date})
            # 数据库里已有最新财报的股票
            done_stocks = [s['ts_code'] for s in list(stocks_with_latest_fi)]
            # 尚未有最新财报的股票
            pending_stocks = []
            for stock in all_stocks:
                if stock not in done_stocks:
                    pending_stocks.append(stock)
            for s in pending_stocks:
                df = ts_pro.fina_indicator(ts_code=s, period=latest_end_date)
                if df.empty:
                    logging.info('最新财报尚未披露: [{}]'.format(s))
                else:
                    stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).insert_many(json.loads(df.T.to_json()).values())


if __name__ == '__main__':
    fic = FinIndCollector()
    # fic.get_all_fin_ind(start='20120101', end='20201231')
    fic.incremental_get_fi()

