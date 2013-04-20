# -*- coding: utf-8 -*-

import time
from twisted.internet.threads import deferToThread
from scrapy import log
from items import WeiboItem, UserItem
from tk_maintain import _default_mongo


MONGOD_HOST = 'localhost'
MONGOD_PORT = 27017


class MongodbPipeline(object):
    """
    insert and update items to mongod
    > use test
    switched to db test
    > db.simple.insert({a: 1})
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1 }
    > db.simple.update({"a": 1}, {$addToSet: {"b": 2}})
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2 ] }
    > db.simple.update({"a": 1}, { $addToSet : { a : { $each : [ 3 , 5 , 6 ] }
    > } })
    Cannot apply $addToSet modifier to non-array
    > db.simple.update({"a": 1}, { $addToSet : { b : { $each : [ 3 , 5 , 6 ] }
    > } })
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2, 3, 5, 6 ] }
    > db.simple.update({"a": 1}, { $addToSet : { b : { $each : [ 3 , 5 , 6 ] }
    > } })
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2, 3, 5, 6 ] }
    > db.simple.update({"a": 1}, { $addToSet : { a : { $each : [ 3 , 5 , 6 ] }
    > } })
    Cannot apply $addToSet modifier to non-array
    > db.simple.update({"a": 1}, {$addToSet: {"b": [2,7]}})
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2, 3, 5, 6, [ 2, 7 ] ] }
    > db.simple.update({"a": 1}, {$set: {"c": []}})
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2, 3, 5, 6, [ 2, 7 ] ], "c" : [ ] }
    > db.simple.update({"a": 1}, { $addToSet : { c : { $each : [ 3 , 5 , 6 ] }
    > } })
    > db.simple.find()
    { "_id" : ObjectId("50af9034d086dd9cd0ba3275"), "a" : 1, "b" : [ 2, 3, 5, 6, [ 2, 7 ] ], "c" : [ 3, 5, 6 ] }
    """

    def __init__(self, host=MONGOD_HOST, port=MONGOD_PORT):
        self.db = _default_mongo(host, port, usedb='master_timeline')
        log.msg('Mongod connect to {host}:{port}'.format(host=host, port=port), level=log.INFO)

    @classmethod
    def from_settings(cls, settings):
        host = settings.get('MONGOD_HOST', MONGOD_HOST)
        port = settings.get('MONGOD_PORT', MONGOD_PORT)
        return cls(host, port)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            return deferToThread(self.process_weibo, item, spider)
        elif isinstance(item, UserItem):
            return deferToThread(self.process_user, item, spider)

    def process_item_sync(self, item, spider):
        if isinstance(item, WeiboItem):
            return self.process_weibo(item, spider)
        elif isinstance(item, UserItem):
            return self.process_user(item, spider)

    def process_weibo(self, item, spider):
        weibo = item.to_dict()
        weibo['_id'] = weibo['id']

        if self.db.master_timeline_weibo.find({'_id': weibo['_id']}).count():
            updates = {}
            updates['last_modify'] = time.time()
            for key in WeiboItem.PIPED_UPDATE_KEYS:
                # 如reposts这项的初始化是依赖[], 所以只是判断了不是None
                if weibo.get(key) is not None:
                    updates[key] = weibo[key]

            # reposts
            updates_modifier = {'$set': updates, '$addToSet': {'reposts': {'$each': weibo['reposts']}}}
            self.db.master_timeline_weibo.update({'_id': weibo['_id']}, updates_modifier)
        else:
            weibo['first_in'] = time.time()
            weibo['last_modify'] = weibo['first_in']
            self.db.master_timeline_weibo.insert(weibo)

        return item

    def process_user(self, item, spider):
        user = item.to_dict()
        user['_id'] = user['id']
        if self.db.master_timeline_user.find({'_id': user['_id']}).count():
            updates = {}
            updates['last_modify'] = time.time()
            for key in UserItem.PIPED_UPDATE_KEYS:
                # 如followers这一项，初始化为[]
                if user.get(key) is not None:
                    updates[key] = user[key]

            updates_modifier = {'$set': updates,
                                '$addToSet': {
                                    'followers': {'$each': user['followers']},
                                    'friends': {'$each': user['friends']}
                                }}
            self.db.master_timeline_user.update({'_id': user['_id']}, updates_modifier)
        else:
            user['first_in'] = time.time()
            user['last_modify'] = user['first_in']
            self.db.master_timeline_user.insert(user)

        return item
