from scrapy.exceptions import CloseSpider
from scrapy import log
from tk_alive import TkAlive
import simplejson as json
import urllib2
import time

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

                resp = urllib2.urlopen(LIMIT_URL.format(access_token=token))
                resp = json.loads(resp.read())

                if 'error' in resp:
                    if resp['error'] == 'expired_token':
                        return EXPIRED_TOKEN
                    if resp['error'] == 'invalid_access_token':
                        return INVALID_ACCESS_TOKEN
                    else:
                        continue

                reset_time_in = resp['reset_time_in_seconds']
                remaining = resp['remaining_user_hits']
                return reset_time_in, remaining
            except urllib2.URLError:
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

        users_insimpleapp = mongo.users.find()
        for user in users_insimpleapp:
            if user['expires_in'] > time.time():
                token = user['access_token']
                expires_in = user['expires_in']
                if token in tokens_inqueue:
                    continue

                tk_alive.hset(token, expires_in)
                if req_count.notexisted(token):
                    req_count.reset(token, 0)
                alive_count += 1
        if alive_count >= at_least:
            return

        raise CloseSpider('TOKENS COUNT NOT REACH AT_LEAST')

    @classmethod
    def update_used(cls, req_count):
        log.msg("[Token Maintain] begin update_used")
        tokens_inqueue = req_count.all_tokens()
        for token in tokens_inqueue:
            _, remaining = cls.token_status(token)
            req_count.reset(token, 1000 - remaining)