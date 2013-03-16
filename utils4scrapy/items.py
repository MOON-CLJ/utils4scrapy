# -*- coding: utf-8 -*-

from scrapy.item import Item, Field


class UserItem(Item):
    id = Field()
    name = Field()
    gender = Field()
    province = Field()
    city = Field()
    location = Field()
    description = Field()
    verified = Field()
    verified_reason = Field()
    verified_type = Field()
    followers_count = Field()  # 粉丝数
    statuses_count = Field()
    friends_count = Field()  # 关注数
    profile_image_url = Field()
    bi_followers_count = Field()  # 互粉数
    followers = Field()  # just ids
    friends = Field()  # just ids
    created_at = Field()

    active = Field()
    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['id', 'name', 'gender', 'province', 'city', 'location',
                      'description', 'verified', 'followers_count',
                      'statuses_count', 'friends_count', 'profile_image_url',
                      'bi_followers_count', 'verified', 'verified_reason', 'verified_type', 'created_at']

    PIPED_UPDATE_KEYS = ['name', 'gender', 'province', 'city', 'location',
                         'description', 'verified', 'followers_count',
                         'statuses_count', 'friends_count', 'profile_image_url',
                         'bi_followers_count', 'verified', 'verified_reason', 'verified_type', 'created_at']

    def __init__(self):
        """
        >>> a = UserItem()
        >>> a
        {'followers': [], 'friends': []}
        >>> a.to_dict()
        {'followers': [], 'friends': []}
        """

        super(UserItem, self).__init__()
        default_empty_arr_keys = ['followers', 'friends']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

        self.setdefault('active', False)

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if type(v) in [UserItem, WeiboItem]:
                d[k] = v.to_dict()
            else:
                d[k] = v
        return d


class WeiboItem(Item):
    created_at = Field()
    timestamp = Field()
    id = Field()
    mid = Field()
    text = Field()
    source = Field()
    reposts_count = Field()
    comments_count = Field()
    attitudes_count = Field()
    bmiddle_pic = Field()
    original_pic = Field()
    geo = Field()
    urls = Field()
    hashtags = Field()
    emotions = Field()
    at_users = Field()
    repost_users = Field()
    user = Field()  # 信息可能过期
    retweeted_status = Field()
    reposts = Field()  # just ids
    comments = Field()  # just ids

    first_in = Field()
    last_modify = Field()

    RESP_ITER_KEYS = ['created_at', 'id', 'mid', 'text', 'source', 'reposts_count',
                      'comments_count', 'attitudes_count', 'geo', 'bmiddle_pic', 'original_pic']
    PIPED_UPDATE_KEYS = ['reposts_count', 'comments_count', 'attitudes_count']

    def __init__(self):
        super(WeiboItem, self).__init__()
        default_empty_arr_keys = ['reposts', 'comments']
        for key in default_empty_arr_keys:
            self.setdefault(key, [])

    def to_dict(self):
        d = {}
        for k, v in self.items():
            if type(v) in [UserItem, WeiboItem]:
                d[k] = v.to_dict()
            else:
                d[k] = v

            """
            elif type(v) == list:
                d[k] = []
                for vv in v:
                    d[k].append(vv.to_dict())
            else:
                d[k] = v
            """

        return d
