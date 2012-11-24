# -*- coding: utf-8 -*-

import time
import math
import simplejson as json
from items import WeiboItem, UserItem
from scrapy.exceptions import DropItem
from repost_status import RepostStatus


WEIBO_KEYS = ['created_at', 'id', 'mid', 'text', 'source', 'reposts_count',
              'comments_count', 'attitudes_count', 'geo']
USER_KEYS = ['id', 'name', 'gender', 'province', 'city', 'location',
             'description', 'verified', 'followers_count',
             'statuses_count', 'friends_count', 'profile_image_url',
             'bi_followers_count', 'verified', 'verified_reason', 'verified_type']


def resp2item_v2(resp, base_weibo=None):
    items = []
    if resp is None or 'deleted' in resp or 'mid' not in resp and 'name' not in resp:
        return items

    if 'mid' in resp:
        weibo = WeiboItem()
        for key in WEIBO_KEYS:
            weibo[key] = resp[key]
        weibo['timestamp'] = local2unix(weibo['created_at'])

        if base_weibo:
            base_weibo['retweeted_status'] = weibo

        items.append(weibo)
        items.extend(resp2item_v2(resp.get('user'), weibo))
        items.extend(resp2item_v2(resp.get('retweeted_status'), weibo))
    else:
        user = UserItem()
        for key in USER_KEYS:
            user[key] = resp[key]

        if base_weibo:
            base_weibo['user'] = user

        items.append(user)
        items.extend(resp2item_v2(resp.get('status')))

    return items


def resp2item(resp):
    """ /statuses/show  api structured data to item"""

    weibo = WeiboItem()
    user = UserItem()

    if 'deleted' in resp:
        raise DropItem('deleted')

    if 'reposts_count' not in resp:
        raise DropItem('reposts_count')

    for k in WEIBO_KEYS:
        weibo[k] = resp[k]

    weibo['timestamp'] = local2unix(weibo['created_at'])

    for k in USER_KEYS:
        user[k] = resp['user'][k]

    weibo['user'] = user

    retweeted_user = None
    if 'retweeted_status' in resp and 'deleted' not in resp['retweeted_status']:
        retweeted_status = WeiboItem()
        retweeted_user = UserItem()

        for k in WEIBO_KEYS:
            retweeted_status[k] = resp['retweeted_status'][k]
        retweeted_status['timestamp'] = local2unix(retweeted_status['created_at'])

        for k in USER_KEYS:
            retweeted_user[k] = resp['retweeted_status']['user'][k]

        retweeted_status['user'] = retweeted_user
        weibo['retweeted_status'] = retweeted_status

    return user, weibo, retweeted_user


def local2unix(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return time.mktime(time.strptime(time_str, time_format))


def reposts2tree(reposts):
    pass


def tree2graph(tree):
    pass


def load_last_page(app, weibo2db, client, id, since_id, reposts_count):
    before_reposts_count = weibo2db.before_reposts_count(id, since_id)
    page = int(math.ceil((reposts_count - before_reposts_count) / 200.0)) + 1
    retry = 0
    while retry < 4:
        retry += 1
        if retry > 1:
            page -= 1
            if page < 1:
                return
        try:
            reposts = client.get('statuses/repost_timeline', id=int(id),
                                 count=200, page=page, since_id=since_id)
            if len(reposts['reposts']) > 0:
                weibo2db.reposts(id, reposts['reposts'])
                return reposts['reposts'][0]['id']
        except Exception, e:
            app.logger.error(e)


def load_reposts(app, weibo2db, redis, client, id, since_id):
    repost_status = RepostStatus(redis, id)

    redis_since_id = repost_status.get_sinceid()
    if redis_since_id is None:
        redis_since_id = 0
    else:
        redis_since_id = int(redis_since_id)

    if since_id < redis_since_id:
        since_id = redis_since_id
    count = repost_status.get_repostcount()
    if count is None:
        try:
            count = reposts_count(app, weibo2db, client, id)
            repost_status.set_repostcount(count)
        except:
            return [], None, 0
    count = int(count)

    new_since_id = load_last_page(app, weibo2db, client, id, since_id, count)
    if new_since_id is not None:
        since_id = new_since_id
        repost_status.set_sinceid(new_since_id)

    reposts, source_weibo = weibo2db.before_reposts(id, since_id)

    return reposts, source_weibo, since_id


def reposts_count(app, weibo2db, client, id):
    retry = 0
    while retry < 3:
        retry += 1
        try:
            source_weibo = client.get('statuses/show', id=int(id))
            weibo2db.status(source_weibo)

            reposts_count = int(source_weibo["reposts_count"])
            if reposts_count > 0:
                return reposts_count
        except Exception, e:
            app.logger.error(e)
    else:
        app.logger.error("get reposts count of %s fail" % id)
        raise Exception("get reposts count of %s fail" % id)

if __name__ == '__main__':
    # 1 public_timeline(weibo里有user)
    """
    weibo_str = r'{"created_at":"Sat Nov 24 14:18:42 +0800 2012","id":3515894033020645,"mid":"3515894033020645","idstr":"3515894033020645","text":"对门老爷爷90大寿。。。长寿面，寿桃。。。讨个吉利！@我不叫张小胡 @我也叫王震彬 @倩倩喵儿 @不入穴焉得子 @果冻莘妈妈 @小猪嘴xixi0","source":"<a href=\"http://www.samsung.com/cn/\" rel=\"nofollow\">三星Galaxy SIII</a>","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","thumbnail_pic":"http://ww2.sinaimg.cn/thumbnail/9339698cjw1dz63yoexqaj.jpg","bmiddle_pic":"http://ww2.sinaimg.cn/bmiddle/9339698cjw1dz63yoexqaj.jpg","original_pic":"http://ww2.sinaimg.cn/large/9339698cjw1dz63yoexqaj.jpg","geo":null,"user":{"id":2470013324,"idstr":"2470013324","screen_name":"miaow喵咪","name":"miaow喵咪","province":"32","city":"1000","location":"江苏","description":"💓我小小的幸福🎀有你的陪伴💏直到永远💍","url":"","profile_image_url":"http://tp1.sinaimg.cn/2470013324/50/5613817207/0","profile_url":"u/2470013324","domain":"","weihao":"","gender":"f","followers_count":593,"friends_count":450,"statuses_count":3922,"favourites_count":190,"created_at":"Sat Oct 15 20:58:18 +0800 2011","following":false,"allow_all_act_msg":false,"geo_enabled":true,"verified":false,"verified_type":220,"allow_all_comment":false,"avatar_large":"http://tp1.sinaimg.cn/2470013324/180/5613817207/0","verified_reason":"","follow_me":false,"online_status":0,"bi_followers_count":153,"lang":"zh-cn","star":0,"mbtype":0,"mbrank":0,"block_word":0},"reposts_count":0,"comments_count":0,"attitudes_count":0,"mlevel":0,"visible":{"type":0,"list_id":0}}'
    """
    """
    # 2 friendships/friends(user里有微博)
    weibo_str = r'{"id":1662047260,"idstr":"1662047260","screen_name":"SinaAppEngine","name":"SinaAppEngine","province":"11","city":"8","location":"北京 海淀区","description":"Sina App Engine（简称SAE），简单高效的分布式Web服务开发、运行平台。\n新浪云平台Sina App Engine官网（...","url":"http://sae.sina.com.cn","profile_image_url":"http://tp1.sinaimg.cn/1662047260/50/5633919323/1","profile_url":"saet","domain":"saet","weihao":"","gender":"m","followers_count":199832,"friends_count":158,"statuses_count":4607,"favourites_count":17,"created_at":"Thu Nov 19 14:47:16 +0800 2009","following":false,"allow_all_act_msg":true,"geo_enabled":true,"verified":true,"verified_type":2,"status":{"created_at":"Sat Nov 24 10:00:12 +0800 2012","id":3515828978513205,"mid":"3515828978513205","idstr":"3515828978513205","text":"【#Web应用开发#】《7 款让人跃跃欲试的 jQuery 超炫插件》jQuery大大简化了我们的前端代码，因为jQuery的简单和开源，也涌现出了层出不穷的jQuery插件，这些实用的jQuery插件也不断推动着jQuery开源社区的发展。下面精选了几款让人跃跃欲试的jQuery实用插件，朋友们赶紧收藏吧。http://t.cn/zjUHL3I","source":"<a href=\"http://sae.sina.com.cn\" rel=\"nofollow\">SAE新浪云计算平台</a>","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","thumbnail_pic":"http://ww2.sinaimg.cn/thumbnail/6310d41cjw1dz506c6lqej.jpg","bmiddle_pic":"http://ww2.sinaimg.cn/bmiddle/6310d41cjw1dz506c6lqej.jpg","original_pic":"http://ww2.sinaimg.cn/large/6310d41cjw1dz506c6lqej.jpg","geo":null,"reposts_count":0,"comments_count":0,"attitudes_count":0,"mlevel":0,"visible":{"type":0,"list_id":0}},"allow_all_comment":true,"avatar_large":"http://tp1.sinaimg.cn/1662047260/180/5633919323/1","verified_reason":"Sina App Engine官方微博","follow_me":false,"online_status":0,"bi_followers_count":118,"lang":"zh-cn","star":0,"mbtype":0,"mbrank":0,"block_word":0}'
    """
    # 3 repost_timeline(weibo里有user和retweet_status, retweet_status里有user)
    weibo_str = r'{"created_at":"Sat Nov 24 14:07:07 +0800 2012","id":3515891121130294,"mid":"3515891121130294","idstr":"3515891121130294","text":"转发微博","source":"<a href=\"\" rel=\"nofollow\">未通过审核应用</a>","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","geo":null,"user":{"id":3075030277,"idstr":"3075030277","screen_name":"xcode2012神探","name":"xcode2012神探","province":"11","city":"8","location":"北京 海淀区","description":"","url":"","profile_image_url":"http://tp2.sinaimg.cn/3075030277/50/0/1","profile_url":"u/3075030277","domain":"","weihao":"","gender":"m","followers_count":22,"friends_count":99,"statuses_count":24,"favourites_count":0,"created_at":"Mon Oct 29 16:40:09 +0800 2012","following":false,"allow_all_act_msg":false,"geo_enabled":true,"verified":false,"verified_type":-1,"remark":"","allow_all_comment":true,"avatar_large":"http://tp2.sinaimg.cn/3075030277/180/0/1","verified_reason":"","follow_me":false,"online_status":1,"bi_followers_count":7,"lang":"zh-cn","star":0,"mbtype":0,"mbrank":0,"block_word":0},"retweeted_status":{"created_at":"Tue Aug 21 14:48:20 +0800 2012","id":3481474642286341,"mid":"3481474642286341","idstr":"3481474642286341","text":"【平台公告】微博开放平台问答系统正式上线！开发者可在本系统提出任何和新浪微博开放平台有关的问题，会有热心的用户及专业客服为您及时解答。问答系统地址：http://t.cn/zWHICRh","source":"<a href=\"http://e.weibo.com\" rel=\"nofollow\">专业版微博</a>","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","thumbnail_pic":"http://ww2.sinaimg.cn/thumbnail/717f7411jw1dw4axx332rj.jpg","bmiddle_pic":"http://ww2.sinaimg.cn/bmiddle/717f7411jw1dw4axx332rj.jpg","original_pic":"http://ww2.sinaimg.cn/large/717f7411jw1dw4axx332rj.jpg","geo":null,"user":{"id":1904178193,"idstr":"1904178193","screen_name":"微博开放平台","name":"微博开放平台","province":"11","city":"8","location":"北京 海淀区","description":"#平台沙龙两周年#每期沙龙都离不开热爱平台的朋友们，您是否记得2010年10月初次相聚，我们一起见证平台启程；两年间，平台与开发者一同发...","url":"","profile_image_url":"http://tp2.sinaimg.cn/1904178193/50/5610154048/0","profile_url":"openapi","domain":"openapi","weihao":"","gender":"f","followers_count":60031,"friends_count":47,"statuses_count":1037,"favourites_count":2,"created_at":"Mon Dec 27 17:56:46 +0800 2010","following":true,"allow_all_act_msg":false,"geo_enabled":true,"verified":true,"verified_type":2,"remark":"","allow_all_comment":false,"avatar_large":"http://tp2.sinaimg.cn/1904178193/180/5610154048/0","verified_reason":"新浪微博开放平台","follow_me":false,"online_status":0,"bi_followers_count":38,"lang":"zh-cn","star":0,"mbtype":0,"mbrank":0,"block_word":0},"reposts_count":615,"comments_count":49,"attitudes_count":0,"mlevel":0,"visible":{"type":0,"list_id":0}},"reposts_count":0,"comments_count":0,"attitudes_count":0,"mlevel":0,"visible":{"type":0,"list_id":0}}'
    weibo = json.loads(weibo_str)
    items = resp2item_v2(weibo)
    for item in items:
        print "** " * 10
        print item
    print len(items)
