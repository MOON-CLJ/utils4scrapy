import time

TK_ALIVE_HASH = "{api_key}:tokensalive"


class TkAlive(object):
    def __init__(self, server, api_key):
        self.server = server
        self.key = TK_ALIVE_HASH.format(api_key=api_key)

    def hset(self, token, expired_in):
        self.server.hset(self.key, token, expired_in)

    def isalive(self, token):
        return float(self.server.hget(self.key, token)) > time.time()

    def drop_tk(self, token):
        if self.isalive(token):
            self.server.hset(self.key, token, time.time())
