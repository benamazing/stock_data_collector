"""
  获取全部股票每日重要的基本面指标
"""

from common import cons
from common.g import ts_pro
from common.g import stockdb


import json
import datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)


class DailyBasicCollector:
    def __init__(self):
        pass

    def get_today_basic(self):
        """
        获取全部股票每日的基本面指标并保存
        :param date:
        :return:
        """
        now = datetime.datetime.now()
        if now.hour < 17:
            logging.info('今天的数据未完成更新，暂停获取')
            return
        today = now.strftime('%Y%m%d')
        df = ts_pro.daily_basic(ts_code='', trade_date=today)
        stockdb.get_collection(cons.S_DAILY_BASIC).insert_many(json.loads(df.T.to_json()).values())

    def check_if_exist(self, day):
        """
        Check if the daily basic data already exists in db for a given day
        :param day:
        :return:
        """
        rst = stockdb.get_collection(cons.S_DAILY_BASIC).count_documents({'trade_date': day})
        if rst == 0 or rst is None:
            return False
        return True

    def get_history_basic(self, start, end):
        """
        获取全部股票历史上某一时间段的基本面指标并保存
        :return:
        """
        start_day = datetime.datetime.strptime(start, '%Y%m%d')
        end_day = datetime.datetime.strptime(end, '%Y%m%d')
        while start_day <= end_day:
            trade_date = start_day.strftime('%Y%m%d')
            df = ts_pro.daily_basic(ts_code='', trade_date=trade_date)
            if df.empty:
                logging.info('Result is empty for {}'.format(trade_date))
            else:
                exist = self.check_if_exist(trade_date)
                if exist:
                    logging.info('Data already exists for {}, will delete it before insert new data'.format(trade_date))
                    stockdb.get_collection(cons.S_DAILY_BASIC).delete_many({"trade_date": trade_date})
                rst = stockdb.get_collection(cons.S_DAILY_BASIC).insert_many(json.loads(df.T.to_json()).values())
                if rst:
                    logging.info('Insert daily basic for {} successfully'.format(trade_date))
                else:
                    logging.error('Insert daily basic for {} failed'.format(trade_date))
            start_day = start_day + datetime.timedelta(days=1)


if __name__ == '__main__':
    dbc = DailyBasicCollector()
    dbc.get_history_basic('20200427', '20200502')
