#! /usr/bin/env python
# -*- coding: utf-8 -*-

import eventlet
from eventlet import wsgi
from xapian_weibo.utils import load_scws
import json
import urllib

JSON_HEADER = [('content-Type', 'application/json;charset=UTF-8'), ("Access-Control-Allow-Origin", "*"), ('Server', 'WDC-eventlet')]

s = load_scws()


def cut(text, f=None):
    global s
    if f:
        return [token[0].decode('utf-8') for token in s.participle(text) if token[1] in f and (token[0].isalnum() or len(token[0]) > 3)]
    else:
        return [token[0].decode('utf-8') for token in s.participle(text) if token[0].isalnum() or len(token[0]) > 3]


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
    wsgi.server(eventlet.listen(('0.0.0.0', 8890)), word_seg, minimum_chunk_size=64)
    return 0

if __name__ == '__main__':
    exit(main())
