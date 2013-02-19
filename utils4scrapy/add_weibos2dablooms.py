# -*- coding: utf-8 -*-

# 将已有master_timeline的微博加入dablooms的集合

import pydablooms
import time
from utils4scrapy.tk_maintain import _default_mongo

MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
DABLOOMS_CAPACITY = 2000000000
DABLOOMS_ERROR_RATE = .001
DABLOOMS_FILEPATH = '/opt/scrapy_weibo/scrapy_weibo/bloom.bin'
#DABLOOMS_FILEPATH = '/tmp/bloom.bin'

bloom = pydablooms.Dablooms(capacity=DABLOOMS_CAPACITY,
                            error_rate=DABLOOMS_ERROR_RATE, filepath=DABLOOMS_FILEPATH)
db = _default_mongo(MONGOD_HOST, MONGOD_PORT, usedb='master_timeline')

for status in db.master_timeline_weibo.find():
    bloom.add(status['mid'], int(time.time() * 1000))
