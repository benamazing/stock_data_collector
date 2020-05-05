"""
 select the stocks based on one single financial indicator
 Created on 2020-05-05
 @author: benamazing
"""

from common.g import stockdb
from common.g import ts_pro
from common import cons
import logging
import sys
from prettytable import PrettyTable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

def is_greater_than(array, key=None, value=0):
    """
    判断一个array里面所有object的某个key是否都大于某个值
    :param array: 要检查的列表
    :param key: 要检查的key
    :param value: 要比较的值
    :return: True if all satisfy, other False
    """
    if array is None:
        return False
    try:
        if not key:
            for item in array:
                if item <= value:
                    return False
            return True
        else:
            for item in array:
                if item.get(key, value) <= value:
                    return False
            return True
    except Exception as e:
        logging.error('Failed to check array is positive or not: {}'.format(e))
        return False


def is_sorted(array, key=None, reverse=False):
    """
    判断一个对象列表是否已按某个key排序
    :param array: 列表
    :param key: 排序的key
    :param reverse: 是否倒叙
    :return: True if sorted, otherwise False
    """
    if array is None or len(array) == 1:
        return True
    try:
        if key:
            sorted_array = sorted(array, key=lambda x:x[key], reverse=reverse)
        else:
            sorted_array = sorted(array, reverse=reverse)
        if array == sorted_array:
            return True
        else:
            return False
    except Exception as e:
        # logging.error('Failed to check if the array is sorted: {}'.format(e))
        return False


def growing_for_years(indicator, years):
    """
    某个指标连续几年递增
    :param indicator: 财务指标
    :param years: 连续多少年
    :return: 满足条件的股票代码列表
    """
    results = []
    stocks = stockdb.get_collection(cons.S_STOCK_LIST).find(projection={'_id': False, 'ts_code': True, 'name': True, 'industry': True})
    for s in stocks:
        ts_code = s['ts_code']
        hist_fi = stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find({'ts_code': ts_code, 'end_date': {"$regex":"1231"}}).sort('end_date', -1).limit(years)
        hist_fi = list(hist_fi)
        if hist_fi:
            rst = is_sorted(hist_fi, key=indicator, reverse=True)
        if rst:
            # 大于0
            rst = is_greater_than(hist_fi, key=indicator, value=0)
            if rst:
                print(s['ts_code'])
                results.append(s['ts_code'])
    return results

def printStocksInfo(stocks):
    p = PrettyTable()
    p.field_names = ['ts_code', 'name', 'industry', 'financial date', 'roe', 'pe_ttm']
    for s in stocks:
        rst = list(stockdb.get_collection(cons.S_STOCK_LIST).find({"ts_code": s}).limit(1))
        if rst:
            name = rst[0]['name']
            industry = rst[0]['industry']
        else:
            name = 'N/A'
            industry = 'N/A'
        rst = list(stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find({'ts_code': s}).sort("end_date", -1).limit(1))
        if rst:
            financial_date = rst[0]['end_date']
            roe = rst[0]['roe']
        else:
            financial_date = 'N/A'
            roe = 'N/A'
        rst = list(stockdb.get_collection(cons.S_DAILY_BASIC).find({'ts_code': s}).sort('trade_date', -1).limit(1))
        if rst:
            pe_ttm = rst[0]['pe_ttm']
        else:
            pe_ttm = 'N/A'
        p.add_row([s, name, industry, financial_date, roe, pe_ttm])
    print(p)


if __name__ == '__main__':
    # 年净利润增速连续4年递增
    stocks = growing_for_years('netprofit_yoy', 5)
    printStocksInfo(stocks)
