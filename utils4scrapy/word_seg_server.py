#! /usr/bin/env python
# -*- coding: utf-8 -*-

import eventlet
import scws
from eventlet import wsgi
import json
import urllib

SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
CUSTOM_DICT_PATH = '../../xapian_weibo/dict/userdic.txt'
IGNORE_PUNCTUATION = 1
EXTRA_STOPWORD_PATH = '../../xapian_weibo/dict/stopword.dic'
EXTRA_EMOTIONWORD_PATH = '../../xapian_weibo/dict/emotionlist.txt'
JSON_HEADER = [('content-Type', 'application/json;charset=UTF-8'), ("Access-Control-Allow-Origin", "*"), ('Server', 'WDC-eventlet')]

s = scws.Scws()
s.set_charset(SCWS_ENCODING)

s.set_dict(CHS_DICT_PATH, scws.XDICT_MEM)
s.add_dict(CHT_DICT_PATH, scws.XDICT_MEM)
s.add_dict(CUSTOM_DICT_PATH, scws.XDICT_TXT)

# 把停用词全部拆成单字，再过滤掉单字，以达到去除停用词的目的
s.add_dict(EXTRA_STOPWORD_PATH, scws.XDICT_TXT)
# 即基于表情表对表情进行分词，必要的时候在返回结果处或后剔除
s.add_dict(EXTRA_EMOTIONWORD_PATH, scws.XDICT_TXT)

s.set_rules(SCWS_RULES)
s.set_ignore(IGNORE_PUNCTUATION)


def cut(text, f=None):
    global s
    if f:
        return [token[0].decode('utf-8') for token in s.participle(text) if token[1] in f]
    else:
        return [token[0].decode('utf-8') for token in s.participle(text)]


def word_seg(env, start_response):
    if env['PATH_INFO'] != '/seg':
        start_response('404 Not Found', [('Content-Type', 'text/plain')])
        return ['Not Found\r\n']

    try:
        query_str = env['QUERY_STRING']
        paras = query_str.split('&')
        text = None
        f = None
        for para in paras:
            key, value = para.split('=')
            if key == 'text':
                text = value
            if key == 'f':
                f = value.split(',')
        text = urllib.unquote(text)
        words = cut(text, f=f)
        start_response('200 OK', JSON_HEADER)
        return json.dumps({'status': 'ok', 'words': words})
    except Exception, e:
        print e
        start_response('500 Internal Server Error', JSON_HEADER)
        return json.dumps({'status': 'error'})


def main():
    wsgi.server(eventlet.listen(('127.0.0.1', 8890)), word_seg, minimum_chunk_size=64)
    return 0

if __name__ == '__main__':
    exit(main())
