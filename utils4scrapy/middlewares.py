from scrapy import log
from scrapy.conf import settings
from tk_maintain import token_status, one_valid_token, \
    _default_redis, _default_req_count, _default_tk_alive, \
    HOURS_LIMIT, EXPIRED_TOKEN, INVALID_ACCESS_TOKEN
import simplejson as json
import time


# weibo apis default extras config
REDIS_HOST = 'localhost'
REDIS_PORT = 6378
API_KEY = '4131380600'
BUFFER_SIZE = 100


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
                level=log.DEBUG, spider=spider, token=token, used=used)
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
        if response.status == 403:
            resp = json.loads(response.body)
            if resp.get('error_code') in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
                token = response.request.headers['Authorization'][7:]
                self.req_count.delete(token)
                self.tk_alive.drop_tk(token)

                reason = resp.get('error')
                log.msg(format='Drop token: %(token)s %(reason)s',
                        level=log.DEBUG, spider=spider, token=token, reason=reason)
