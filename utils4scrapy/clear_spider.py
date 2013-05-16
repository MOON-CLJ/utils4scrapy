# -*- coding: utf-8 -*-

# 手动清空requests队列和去重集合的脚本
# usage: py clear_spider.py public_timeline [st]

import sys
import redis
from tk_maintain import _default_redis

#dev
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

#prod
"""
REDIS_HOST = '219.224.135.60'
REDIS_PORT = 6379
"""

QUEUE_KEY = '%(spider)s:requests'
DUPEFILTER_KEY = '%(spider)s:dupefilter'

spider_name = sys.argv[1]
not_del = False
if len(sys.argv) > 2 and sys.argv[2] == 'st':
    not_del = True

r = _default_redis(REDIS_HOST, REDIS_PORT)

try:
    print 'scheduled requests: %s' % r.llen(QUEUE_KEY % {'spider': spider_name})
except redis.exceptions.ResponseError:
    print 'scheduled requests: %s' % r.zcard(QUEUE_KEY % {'spider': spider_name})

print 'dupefiler requests: %s' % r.scard(DUPEFILTER_KEY % {'spider': spider_name})

if not not_del:
    r.delete(QUEUE_KEY % {'spider': spider_name})
    r.delete(DUPEFILTER_KEY % {'spider': spider_name})
    print 'clear %s' % spider_name
