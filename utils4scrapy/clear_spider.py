# -*- coding: utf-8 -*-
# usage: py clear_spider.py public_timeline [st]

import sys
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

host = REDIS_HOST
port = REDIS_PORT
r = _default_redis(host, port)

print 'scheduled requests: %s' % r.zcard(QUEUE_KEY % {'spider': spider_name})
print 'dupefiler requests: %s' % r.scard(DUPEFILTER_KEY % {'spider': spider_name})

if not not_del:
    r.delete(QUEUE_KEY % {'spider': spider_name})
    r.delete(DUPEFILTER_KEY % {'spider': spider_name})
    print 'clear %s' % spider_name
