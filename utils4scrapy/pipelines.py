import pymongo
import time
from scrapy.conf import settings
from scrapy import log
from items import WeiboItem, UserItem


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

    def __init__(self):
        host = settings.get("MONGOD_HOST", MONGOD_HOST)
        port = settings.get("MONGOD_PORT", MONGOD_PORT)
        connection = pymongo.Connection(host, port)
        db = connection.admin
        db.authenticate('root', 'root')
        log.msg('Mongod connect to {host}:{port}'.format(host=host, port=port), level=log.WARNING)

        db = connection.master_timeline
        self.db = db

    def process_item(self, item, spider):
        if isinstance(item, WeiboItem):
            self.process_weibo(item)
        elif isinstance(item, UserItem):
            self.process_user(item)

        return item

    def process_weibo(self, item):
        weibo = item.to_dict()
        weibo['_id'] = weibo['id']

        if self.db.master_timeline_weibo.find({'_id': weibo['_id']}).count():
            update_keys = ['reposts_count', 'comments_count', 'attitudes_count']
            updates = {}
            updates['last_modify'] = time.time()
            for key in update_keys:
                if weibo.get(key) is not None:
                    updates[key] = weibo[key]

            # reposts
            updates_modifier = {'$set': updates, '$addToSet': {'reposts': {'$each': weibo['reposts']}}}
            self.db.master_timeline_weibo.update({'_id': weibo['_id']}, updates_modifier)
        else:
            weibo['first_in'] = time.time()
            weibo['last_modify'] = weibo['first_in']
            self.db.master_timeline_weibo.insert(weibo)

    def process_user(self, item):
        user = item.to_dict()
        user['_id'] = user['id']
        if self.db.master_timeline_user.find({'_id': user['_id']}).count():
            update_keys = ['name', 'gender', 'province', 'city',
                           'location', 'description', 'verified', 'followers_count',
                           'statuses_count', 'friends_count', 'profile_image_url',
                           'bi_followers_count', 'verified', 'verified_reason', 'verified_type', 'created_at']
            updates = {}
            updates['last_modify'] = time.time()
            for key in update_keys:
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
