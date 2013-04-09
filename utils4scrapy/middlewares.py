# -*- coding: utf-8 -*-

from scrapy import log
from scrapy.exceptions import CloseSpider
from tk_maintain import token_status, one_valid_token, calibration, \
    _default_redis, _default_req_count, _default_tk_alive, \
    EXPIRED_TOKEN, INVALID_ACCESS_TOKEN, REACH_IP_LIMIT, REACH_PER_TOKEN_LIMIT, REACH_PER_TOKEN_LIMIT_1
from raven.handlers.logging import SentryHandler
from raven import Client
from raven.conf import setup_logging
from scrapy.utils.reqser import request_to_dict
import cPickle
import logging
import simplejson as json
import time
import sys


BUFFER_SIZE = 100

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
    def __init__(self, host, port, api_key, per_token_hours_limit, buffer_size):
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r, api_key=api_key)
        self.per_token_hours_limit = per_token_hours_limit
        self.buffer_size = buffer_size

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get('REDIS_HOST')
        port = settings.get('REDIS_PORT')
        api_key = settings.get('API_KEY')
        per_token_hours_limit = settings.get('PER_TOKEN_HOURS_LIMIT')
        buffer_size = settings.get('BUFFER_SIZE')
        return cls(host, port, api_key, per_token_hours_limit, buffer_size)

    def process_request(self, request, spider):
        token_and_used = one_valid_token(self.req_count, self.tk_alive)
        if token_and_used is None:
            log.msg(format='No token alive',
                    level=log.INFO, spider=spider)

            raise CloseSpider('No Token Alive')
        token, used = token_and_used

        if used > self.per_token_hours_limit - self.buffer_size:
            calibration(self.req_count, self.tk_alive, self.per_token_hours_limit)
            token, _ = one_valid_token(self.req_count, self.tk_alive)
            tk_status = token_status(token)
            reset_time_in, remaining = tk_status
            if remaining < BUFFER_SIZE:
                log.msg(format='REACH API REQUEST BUFFER, SLEEP %(reset_time_in)s SECONDS',
                        level=log.WARNING, spider=spider, reset_time_in=reset_time_in)

                time.sleep(reset_time_in)

        log.msg(format='Request token: %(token)s used: %(used)s',
                level=log.INFO, spider=spider, token=token, used=used)
        request.headers['Authorization'] = 'OAuth2 %s' % token


class ErrorRequestMiddleware(object):
    def __init__(self, host, port, api_key):
        r = _default_redis(host, port)
        self.req_count = _default_req_count(r, api_key=api_key)
        self.tk_alive = _default_tk_alive(r, api_key=api_key)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        host = settings.get('REDIS_HOST')
        port = settings.get('REDIS_PORT')
        api_key = settings.get('API_KEY')
        return cls(host, port, api_key)

    def process_spider_input(self, response, spider):
        resp = json.loads(response.body)
        if response.status == 403:
            reason = resp.get('error')
            if resp.get('error_code') in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
                token = response.request.headers['Authorization'][7:]
                self.req_count.delete(token)
                self.tk_alive.drop_tk(token)
                log.msg(format='Drop token: %(token)s %(reason)s',
                        level=log.INFO, spider=spider, token=token, reason=reason)

            if resp.get('error_code') in [REACH_IP_LIMIT, REACH_PER_TOKEN_LIMIT, REACH_PER_TOKEN_LIMIT_1]:
                log.msg(format='REACH API LIMIT, SLEEP 60*60 SECONDS %(error)s %(error_code)s',
                        level=log.WARNING, spider=spider, error=resp.get('error'), error_code=resp.get('error_code'))

                time.sleep(3600)

            raise InvalidTokenError('%s %s' % (token, reason))

        elif resp.get('error'):
            log.msg(format='UnknownResponseError: %(error)s %(error_code)s',
                    level=log.ERROR, spider=spider, error=resp.get('error'), error_code=resp.get('error_code'))

            raise UnknownResponseError('%s %s' % (resp.get('error'), resp.get('error_code')))


class RetryErrorResponseMiddleware(object):
    def __init__(self, retry_times):
        self.retry_times = retry_times

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        retry_times = settings.get('RETRY_TIMES', 2)
        return cls(retry_times)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        if retries <= self.retry_times:
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
    def __init__(self, sentry_dsn):
        client = Client(sentry_dsn, string_max_length=sys.maxint)

        handler = SentryHandler(client)
        setup_logging(handler)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        sentry_dsn = settings.get('SENTRY_DSN')
        return cls(sentry_dsn)

    def process_spider_exception(self, response, exception, spider):
        self.logger.error('SentrySpiderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
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
    def __init__(self, sentry_dsn):
        client = Client(sentry_dsn, string_max_length=sys.maxint)

        handler = SentryHandler(client)
        setup_logging(handler)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        sentry_dsn = settings.get('SENTRY_DSN')
        return cls(sentry_dsn)

    def process_exception(self, request, exception, spider):
        self.logger.error('SentryDownloaderMiddleware %s [%s]' % (exception, spider.name), exc_info=True, extra={
            'culprit': 'SentryDownloaderMiddleware/%s [spider: %s]' % (type(exception), spider.name),
            'stack': True,
            'data': {
                'request': cPickle.dumps(request_to_dict(request, spider)),
                'exception': exception,
                'spider': spider,
            }
        })
