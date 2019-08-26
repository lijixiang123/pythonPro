#coding=utf-8
import time
import hashlib
import requests
import json

APP_ID = 'DQC'
APP_SECRET = '7cb959f34d53396f7e0f4156cb6a46df'
url_alert = "https://oa.qutoutiao.net/api/open-api/check-sign"
timest = int(time.time())
headers={
    'Content-Type':'application/x-www-form-urlencoded'
}
body = {

    "appId":'DQC',
    'timestamp':str(timest)

}

cmda = sorted(body.items(), key=lambda d: d[0])
str = APP_SECRET
for key in cmda:
    print(key[0])
    print(key[1])
    str = '%s&%s=%s' % (str, key[0], key[1])
    print(str)
body['sign'] = hashlib.md5(str.encode(encoding='UTF-8')).hexdigest()
print body
ret = requests.post(url_alert, data=body,headers=headers)

print(ret.content)