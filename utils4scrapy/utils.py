# -*- coding: utf-8 -*-

import time
import simplejson as json
from items import WeiboItem, UserItem


def resp2item_v2(resp, base_weibo=None, base_user=None):
    items = []
    if resp is None or 'deleted' in resp or 'mid' not in resp and 'name' not in resp:
        return items

    if 'mid' in resp:
        weibo = WeiboItem()
        for key in WeiboItem.RESP_ITER_KEYS:
            if key in resp:
                weibo[key] = resp[key]
        if 'user' not in weibo:
            weibo['user'] = base_user
        weibo['timestamp'] = local2unix(weibo['created_at'])

        if base_weibo:
            base_weibo['retweeted_status'] = weibo

        items.append(weibo)
        items.extend(resp2item_v2(resp.get('user'), base_weibo=weibo))
        items.extend(resp2item_v2(resp.get('retweeted_status'), base_weibo=weibo))
    else:
        user = UserItem()
        for key in UserItem.RESP_ITER_KEYS:
            user[key] = resp[key]

        if base_weibo:
            base_weibo['user'] = user

        items.append(user)
        items.extend(resp2item_v2(resp.get('status'), base_user=user))

    return items


def local2unix(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return time.mktime(time.strptime(time_str, time_format))


if __name__ == '__main__':
    import urllib2

    access_token = '2.00OGiDACguGB4B084c90d75dJELeHB'
    """
    # 1 public_timeline(weibo里有user)
    url = 'https://api.weibo.com/2/statuses/public_timeline.json?access_token=%s' % access_token
    resp = urllib2.urlopen(url).read()
    resp = json.loads(resp)
    resp = resp['statuses'][0]
    """
    """
    # 2 friendships/friends(user里有微博)
    url = 'https://api.weibo.com/2/friendships/friends.json?uid=1870632073&access_token=%s&trim_status=0' % access_token
    resp = urllib2.urlopen(url).read()
    resp = json.loads(resp)
    resp = resp['users'][0]
    """
    """
    # 3 repost_timeline(weibo里有user和retweet_status, retweet_status里有user)
    url = 'https://api.weibo.com/2/friendships/friends.json?uid=1870632073&access_token=%s&trim_status=0' % access_token
    url = 'https://api.weibo.com/2/statuses/repost_timeline.json?id=3481474642286341&access_token=%s' % access_token
    resp = urllib2.urlopen(url).read()
    resp = json.loads(resp)
    resp = resp['reposts'][0]
    """
    """
    # 4 /users/show(user里有微博，但是weibo的字段里缺user这个字段)
    url = 'https://api.weibo.com/2/users/show.json?uid=1904178193&access_token=%s' % access_token
    resp = urllib2.urlopen(url).read()
    resp = json.loads(resp)
    """

    # 5 /statuses/show(类似repost_timeline)
    url = 'https://api.weibo.com/2/statuses/show.json?id=3481475946781445&access_token=%s' % access_token
    resp = urllib2.urlopen(url).read()
    resp = json.loads(resp)

    items = resp2item_v2(resp)
    for item in items:
        print "** " * 10
        if isinstance(item, UserItem):
            if item.keys().sort() != UserItem.RESP_ITER_KEYS.sort():
                print item.keys()
                raise
        elif isinstance(item, WeiboItem):
            if item.keys().sort() != WeiboItem.RESP_ITER_KEYS.sort():
                print item.keys()
                raise
    print len(items)
