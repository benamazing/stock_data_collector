"""
  Select stocks by profit related methods
"""

from common.g import stockdb
from common import cons


def profit_keep_growing(count, q_or_y='quarter', deduct=True):
    """
    Get the stocks whose net profit keep growing for <count> years/quarters
    :param count: 连续多少年或者季度
    :param q_or_y: quarter or year, 年或者季度
    :param deduct: 净利润是否扣非，True为扣非，False为不扣非
    :return: stock list
    """
    results = []
    stocks = stockdb.get_collection(cons.S_STOCK_LIST).find(projection={'_id': False, 'ts_code': True, 'name': True, 'industry': True})
    for s in stocks:
        ts_code = s['ts_code']
        if q_or_y == 'year':
            hist_ind = stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find(filter={'ts_code': ts_code, 'end_date': {"$regex":"1231"}}).sort('end_date', -1).limit(count)
        else:
            hist_ind = stockdb.get_collection(cons.S_FINANCIAL_INDICATOR).find(filter={'ts_code': ts_code}).sort('end_date', -1).limit(count)
        hist_ind = list(hist_ind)
        if deduct:
            result = isSortedAndPositive(hist_ind, key='dt_netprofit_yoy', reverse=True)
        else:
            result = isSortedAndPositive(hist_ind, key='netprofit_yoy', reverse=True)
        if result:
            results.append(s['ts_code'])
            rs = stockdb.get_collection(cons.S_DAILY_BASIC).find({"ts_code": s['ts_code']}).sort("trade_date", -1)
            if rs:
                pe_ttm = rs[0]['pe_ttm']
            else:
                pe_ttm = 'N/A'
            print('{} {} {} {} {} {}'.format(s['ts_code'], s['name'], s['industry'], hist_ind[0]['end_date'], hist_ind[0]['roe'], pe_ttm))
    return results


def isSortedAndPositive(lst, key=None, reverse=False):
    """
    Check whether a list is sorted by key, and the key value is positive
    :param lst:
    :return:
    """
    if lst is None or len(lst) == 1:
        return True
    try:
        if key:
            sorted_l = sorted(lst, key=lambda x: x[key], reverse=reverse)
        else:
            sorted_l = sorted(lst, reverse=reverse)
        for s in sorted_l:
            if s[key] < 0:
                return False
        if lst == sorted_l:
            return True
        else:
            return False
    except Exception as e:
        return False


if __name__ == '__main__':
    profit_keep_growing(4, q_or_y='year', deduct=False)
