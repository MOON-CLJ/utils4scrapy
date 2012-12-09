# -*- coding: utf-8 -*-
from scrapy.exceptions import CloseSpider
from scrapy import log
from tk_alive import TkAlive
from req_count import ReqCount
import redis
import pymongo
import simplejson as json
import urllib3
import time
import socket

LIMIT_URL = 'https://api.weibo.com/2/account/rate_limit_status.json?access_token={access_token}'
EXPIRED_TOKEN = 21327
INVALID_ACCESS_TOKEN = 21332
HOURS_LIMIT = 1000
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017
API_KEY = '4131380600'


def _default_mongo(host=MONGOD_HOST, port=MONGOD_PORT, collection='simple'):
    connection = pymongo.Connection(host, port)
    db = connection.admin
    db.authenticate('root', 'root')
    db = getattr(connection, collection)
    return db


def _default_redis(host=REDIS_HOST, port=REDIS_PORT):
    return redis.Redis(host, port)


def _default_req_count(r=None, api_key=API_KEY):
    if r is None:
        r = _default_redis()

    return ReqCount(r, api_key)


def _default_tk_alive(r=None, api_key=API_KEY):
    if r is None:
        r = _default_redis()

    return TkAlive(r, api_key)


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
                elif resp['error'] == 'invalid_access_token':
                    return INVALID_ACCESS_TOKEN
                else:
                    raise CloseSpider('UNKNOWN TOKEN STATUS ERROR')

            reset_time_in = resp['reset_time_in_seconds']
            remaining = resp['remaining_user_hits']
            return reset_time_in, remaining
        except socket.gaierror:
            pass


def maintain(mongo=None, req_count=None, tk_alive=None, at_least=1, hourly=False, logbk=None):
    if mongo is None:
        mongo = _default_mongo()
    if req_count is None:
        req_count = _default_req_count()
    if tk_alive is None:
        tk_alive = _default_tk_alive()

    log.msg('[Token Maintain] begin maintain')

    # 从应用导入所有未过期的token，并初始使用次数为0，相应的alive为True
    for user in mongo.users.find():
        if user['expires_in'] > time.time():
            req_count.set(user['access_token'], 0)
            tk_alive.hset(user['access_token'], user['expires_in'])

    tokens_in_redis = req_count.all_tokens()
    print 'before alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('before alive: %s' % len(tokens_in_redis))  # 清理之前

    alive_count = 0
    for token in tokens_in_redis:
        if tk_alive.isalive(token, hourly=True):
            alive_count += 1
        else:
            req_count.delete(token)
            tk_alive.drop_tk(token)

    tokens_in_redis = req_count.all_tokens()
    print 'after alive:', len(tokens_in_redis)
    if logbk:
        logbk.info('after alive: %s' % len(tokens_in_redis))  # 清理之后

    if alive_count < at_least:
        raise CloseSpider('TOKENS COUNT NOT REACH AT_LEAST')

    log.msg('[Token Maintain] end maintain')


def calibration(req_count, tk_alive):
    log.msg('[Token Maintain] begin calibration')
    tokens_in_redis = req_count.all_tokens()
    for token in tokens_in_redis:
        tk_status = token_status(token)
        if tk_status in [EXPIRED_TOKEN, INVALID_ACCESS_TOKEN]:
            req_count.delete(token)
            tk_alive.drop_tk(token)
            continue

        _, remaining = tk_status
        req_count.set(token, HOURS_LIMIT - remaining)

    log.msg('[Token Maintain] end calibration')


def one_valid_token(req_count, tk_alive):
    while 1:
        token, used = req_count.one_token()
        if not token:
            raise CloseSpider('No Token Alive')
        elif token and not tk_alive.isalive(token):
            req_count.delete(token)
            tk_alive.drop_tk(token)
            continue

        return token, used


if __name__ == '__main__':
    from logbook import FileHandler
    from logbook import Logger
    from argparse import ArgumentParser
    import sys

    parser = ArgumentParser()
    parser.add_argument('--log', nargs=1, help='log path')
    args = parser.parse_args(sys.argv[1:])
    log_handler = FileHandler(args.log[0])
    logbk = Logger('Token Maintain')

    with log_handler.applicationbound():
        logbk.info('maintain prepare')

        at_least = 6

        logbk.info('maintain begin')
        maintain(at_least=at_least, hourly=True, logbk=logbk)
        logbk.info('maintain end')
