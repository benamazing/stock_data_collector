"""
  Created on 2020-05-02
  @author: benamazing
  Global variables and functions
"""

import tushare as ts
import pymongo
import logging


# set token，可以不设，直接写在~/tk.csv里面
# TOKEN = 'abcde'
# ts.set_token(TOKEN)

ts_pro = ts.pro_api()

__mongohost = '192.168.3.109'
__port = 27017
__dbname = 'stock_data'

mongo_cli = pymongo.MongoClient(host=__mongohost, port=__port)
stockdb = mongo_cli.get_database(__dbname)