# -*- coding: utf-8 -*-

from scrapy import log
from scrapy.conf import settings
from tk_maintain import token_status, one_valid_token, \
    _default_redis, _default_req_count, _default_tk_alive, \
    HOURS_LIMIT, EXPIRED_TOKEN, INVALID_ACCESS_TOKEN
from raven.handlers.logging import SentryHandler
from raven import Client
from raven.conf import setup_logging
from scrapy.utils.reqser import request_to_dict
import cPickle
import logging
import simplejson as json
import time
import sys


# weibo apis default extras config
REDIS_HOST = 'localhost'
REDIS_PORT = 6378
API_KEY = '4131380600'
BUFFER_SIZE = 100
SENTRY_DSN_PROD = 'http://3349196dad314183ba8e07edcd95b884:feb54ca50ead45d2bef6e6571cf76229@219.224.135.60:9000/2'
client = Client(settings.get('SENTRY_DSN', SENTRY_DSN_PROD), string_max_length=sys.maxint)

handler = SentryHandler(client)
setup_logging(handler)
logger = logging.getLogger(__name__)

"""
raise ShouldNotEmptyError() in spider or spidermiddleware's process_spider_input()
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:46:56+0800 [public_timeline] DEBUG: Retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 1 times):
2013-01-25 00:46:56+0800 [public_timeline] DEBUG: Request token: used: 526.0
2013-01-25 00:46:58+0800 [public_timeline] DEBUG: Crawled (200) <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (referer: None)
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:46:59+0800 [public_timeline] DEBUG: Retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 2 times):
2013-01-25 00:46:59+0800 [public_timeline] DEBUG: Request token: used: 527.0
2013-01-25 00:47:01+0800 [public_timeline] DEBUG: Crawled (200) <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (referer: None)
********************
raise error
** ** ** ** ** ** ** ** ** **
SentrySpiderMiddleware process_spider_exception
** ** ** ** ** ** ** ** ** **
RetryErrorResponseMiddleware process_spider_exception
2013-01-25 00:47:01+0800 [public_timeline] DEBUG: Gave up retrying <GET https://api.weibo.com/2/statuses/public_timeline.json?count=200> (failed 3 times):
"""


class InvalidTokenError(Exception):
    """token过期或不合法"""


class UnknownResponseError(Exception):
    """未处理的错误"""


class ShouldNotEmptyError(Exception):
    """返回不应该为空，但是为空了，在spider里抛出"""


class RequestTokenMiddleware(object):
    def __init__(self):
        host = settings.get('REDIS_HOST', REDIS_HOST)
        port = settings.get('REDIS_PORT', REDIS_PORT)
        api_key = settings.get('API_KEY', API_KEY)
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r=r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r=r, api_key=api_key)

    def process_request(self, request, spider):
        token, used = one_valid_token(self.req_count, self.tk_alive)

        if used > HOURS_LIMIT - BUFFER_SIZE:
            while 1:
                tk_status = token_status(token)
                if tk_status in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
                    self.req_count.delete(token)
                    self.tk_alive.drop_tk(token)
                    token, used = one_valid_token(self.req_count, self.tk_alive)
                else:
                    break

            reset_time_in, remaining = tk_status
            if remaining < BUFFER_SIZE:
                log.msg(format='REACH API LIMIT, SLEEP %(reset_time_in)s SECONDS',
                        level=log.WARNING, spider=spider, token=token, used=used)

                time.sleep(reset_time_in)

        log.msg(format='Request token: %(token)s used: %(used)s',
                level=log.INFO, spider=spider, token=token, used=used)
        request.headers['Authorization'] = 'OAuth2 %s' % token


class ErrorRequestMiddleware(object):
    def __init__(self):
        host = settings.get('REDIS_HOST', REDIS_HOST)
        port = settings.get('REDIS_PORT', REDIS_PORT)
        api_key = settings.get('API_KEY', API_KEY)
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r=r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r=r, api_key=api_key)

    def process_spider_input(self, response, spider):
        resp = json.loads(response.body)
        if response.status == 403 and resp.get('error_code') in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
            token = response.request.headers['Authorization'][7:]
            self.req_count.delete(token)
            self.tk_alive.drop_tk(token)

            reason = resp.get('error')
            log.msg(format='Drop token: %(token)s %(reason)s',
                    level=log.INFO, spider=spider, token=token, reason=reason)

            raise InvalidTokenError('%s %s' % (token, reason))

        elif resp.get('error'):
            raise UnknownResponseError('%s %s' % (resp.get('error'), resp.get('error_code')))


class RetryErrorResponseMiddleware(object):
    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        if retries <= settings.get('RETRY_TIMES', 2):
            log.msg(format="Retrying %(request)s (failed %(retries)d times): %(reason)s",
                    level=log.WARNING, spider=spider, request=request, retries=retries, reason=reason)
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            return retryreq
        else:
            log.msg(format="Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                    level=log.ERROR, spider=spider, request=request, retries=retries, reason=reason)

    def process_spider_exception(self, response, exception, spider):
        if 'dont_retry' not in response.request.meta and \
                isinstance(exception, InvalidTokenError) or isinstance(exception, UnknownResponseError) \
                or isinstance(exception, ShouldNotEmptyError):
            return [self._retry(response.request, exception, spider)]


class SentrySpiderMiddleware(object):
    def process_spider_exception(self, response, exception, spider):
        logger.error('SentrySpiderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
            'culprit': 'SentrySpiderMiddleware/%s [spider: %s]' % (type(exception), spider.name),
            'stack': True,
            'data': {
                'response': cPickle.dumps(response.body),
                'request': cPickle.dumps(request_to_dict(response.request, spider)),
                'exception': exception,
                'spider': spider,
            }
        })


class SentryDownloaderMiddleware(object):
    def process_exception(self, request, exception, spider):
        logger.error('SentryDownloaderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
            'culprit': 'SentryDownloaderMiddleware/%s [spider: %s]' % (type(exception), spider.name),
            'stack': True,
            'data': {
                'request': cPickle.dumps(request_to_dict(request, spider)),
                'exception': exception,
                'spider': spider,
            }
        })
