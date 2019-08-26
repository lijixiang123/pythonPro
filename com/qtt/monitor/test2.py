# -*- coding: utf-8 -*-
# @Time    : 2019/7/4 2:49 PM
# @Author  : 流沙
# @File    : 111.py
import requests
import urlparse
import urllib
import time
import hashlib
import collections


url = "https://oa.qutoutiao.net/api/open-api/get-user-by-email"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
appSecret = '7cb959f34d53396f7e0f4156cb6a46df'
appId = 'DQC'
timestamp = int(time.time())


# 生成签名
def signature(params):
    params['appSecret'] = appSecret
    # 字典转为有序字典进行升序排序
    dic_sorted = collections.OrderedDict(sorted(params.items()))
    data = urllib.urlencode(dic_sorted)
    str_value = urllib.unquote(data).encode('utf-8')
    # 生成32位小写md5字符串
    md5_obj = hashlib.md5()
    md5_obj.update(str_value)
    sign = md5_obj.hexdigest()
    return sign


# 验证签名
def sign_ver(sign,params):
    params['sign'] = sign
    # 字典转为有序字典进行升序排序
    dicv = collections.OrderedDict(sorted(params.items()))
    value = urllib.urlencode(dicv)
    str_value = urllib.unquote(value)
    r = requests.post(url=url, data=str_value, headers=HEADERS)
    return r.json()


# 根据工号获取用户信息
def get_user(email):
    params = {
        "appId": appId,
        "timestamp": str(timestamp),
        "email":email
    }
    sign = signature(params)
    # 验证接口请求状态
    api_status = sign_ver(sign,params)
    if api_status['retCode'] != 0:
        return api_status
    params['sign'] = sign
    # 字典转为有序字典进行升序排序
    dic_sorted = collections.OrderedDict(sorted(params.items()))
    req_data = urllib.urlencode(dic_sorted)
    str_value = urllib.unquote(req_data)
    r = requests.post(url=url, data=str_value, headers=HEADERS)
    return r.json()

a = get_user("lijixiang@qutoutiao.net")
print(a)