from tk_alive import TkAlive

REQ_COUNT_HASH = "{api_key}:tokens"


class ReqCount(object):
    def __init__(self, server, api_key):
        self.server = server
        self.key = REQ_COUNT_HASH.format(api_key=api_key)
        self.tk_alive = TkAlive(server, api_key)

    def one_token(self):
        member = self.server.zrange(self.key, 0, 0)
        if member == []:
            return None, None

        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zincrby(self.key, member[0], 1).zscore(self.key, member[0])
        _, score = pipe.execute()
        return member[0], score

    def reset(self, token, count):
        self.server.zadd(self.key, token, count)

    def all_tokens(self):
        return self.server.zrange(self.key, 0, -1)

    def delete(self, token):
        self.server.zrem(self.key, token)
        self.tk_alive.drop_tk(token)

    def notexisted(self, token):
        return self.server.zrank(self.key, token) is None
