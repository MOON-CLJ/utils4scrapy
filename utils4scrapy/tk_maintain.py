from scrapy.exceptions import CloseSpider
from scrapy import log
from tk_alive import TkAlive
import simplejson as json
import urllib3
import time
import socket

LIMIT_URL = 'https://api.weibo.com/2/account/rate_limit_status.json?access_token={access_token}'
EXPIRED_TOKEN = 21327
INVALID_ACCESS_TOKEN = 21332


class TkMaintain(object):
    @staticmethod
    def token_status(token):
        retry = 0
        while 1:
            retry += 1
            if retry > 3:
                raise CloseSpider('CHECK LIMIT STATUS FAIL')

            try:
                log.msg("[Token Status] token: {token}, sleep one second, wait to check".format(token=token))
                time.sleep(1)
                log.msg("[Token Status] now check")

                http = urllib3.PoolManager()
                resp = http.request('GET', LIMIT_URL.format(access_token=token))
                resp = json.loads(resp.data)

                if 'error' in resp:
                    if resp['error'] == 'expired_token':
                        return EXPIRED_TOKEN
                    if resp['error'] == 'invalid_access_token':
                        return INVALID_ACCESS_TOKEN

                reset_time_in = resp['reset_time_in_seconds']
                remaining = resp['remaining_user_hits']
                return reset_time_in, remaining
            except socket.gaierror:
                pass

    @staticmethod
    def maintain(r, mongo, api_key, req_count, at_least=6):
        log.msg("[Token Maintain] begin maintain")
        tk_alive = TkAlive(r, api_key)
        tokens_inqueue = req_count.all_tokens()
        alive_count = 0
        for token in tokens_inqueue:
            # this check because long time no spider is active
            if tk_alive.isalive(token):
                alive_count += 1
            else:
                req_count.delete(token)
        if alive_count >= at_least:
            return

        tokens_inqueue = req_count.all_tokens()
        users_insimpleapp = mongo.users.find()
        for user in users_insimpleapp:
            if user['expires_in'] > time.time():
                token = user['access_token']
                expires_in = user['expires_in']
                if token in tokens_inqueue:
                    continue

                tk_alive.hset(token, expires_in)
                if req_count.notexisted(token):
                    req_count.reset(token, 0)  # this count will be set in update_used
                alive_count += 1
        if alive_count >= at_least:
            return

        raise CloseSpider('TOKENS COUNT NOT REACH AT_LEAST')

    @classmethod
    def update_used(cls, req_count):
        log.msg("[Token Maintain] begin update_used")
        tokens_inqueue = req_count.all_tokens()
        for token in tokens_inqueue:
            token_status = cls.token_status(token)
            if token_status in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
                req_count.delete(token)
                continue

            _, remaining = token_status
            req_count.reset(token, 1000 - remaining)
