#! /usr/bin/env python
# -*- coding: utf-8 -*-

import urllib
import json

text = u'江西幼儿园校车事故遇难者家属获赔48万'
text = urllib.quote(text.encode('utf-8'))

res = urllib.urlopen('http://127.0.0.1:8890/seg?text=%s&f=n,nr,ns,nt' % text)
data = res.read()
data = json.loads(data)
if data['status'] == 'ok':
    for word in data['words']:
        print word
else:
    print data['status']
