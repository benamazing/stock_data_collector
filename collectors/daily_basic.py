"""
  获取全部股票每日重要的基本面指标
"""

from common.ts import ts_pro
from common.mongo import stock_mongo
import json
import time
import datetime

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
            print('今天的数据未完成更新，暂停获取')
            return
        today = now.strftime('%Y%m%d')
        df = ts_pro.daily_basic(ts_code='', trade_date=today)
        stock_mongo.insert_many('daily_basic', json.loads(df.T.to_json()).values())

    def get_history_basic(self, start, end):
        """
        获取全部股票历史上某一时间段的基本面指标并保存
        :return:
        """
        start_day = datetime.datetime.strptime(start, '%Y%m%d')
        end_day = datetime.datetime.strptime(end, '%Y%m%d')
        while start_day <= end_day:
            df = ts_pro.daily_basic(ts_code='', trade_date=start_day.strftime('%Y%m%d'))
            stock_mongo.insert_many('daily_basic', json.loads(df.T.to_json()).values())
            start_day = start_day + datetime.timedelta(days=1)


if __name__ == '__main__':
    dbc = DailyBasicCollector()
    dbc.get_today_basic()
    dbc.get_history_basic('20200429', '20200429')
