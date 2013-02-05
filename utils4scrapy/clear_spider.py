# -*- coding: utf-8 -*-

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
host = REDIS_HOST
port = REDIS_PORT
r = _default_redis(host, port)
r.delete(QUEUE_KEY % {'spider': spider_name})
r.delete(DUPEFILTER_KEY % {'spider': spider_name})
print 'clear %s' % spider_name
