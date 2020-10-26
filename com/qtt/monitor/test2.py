#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import datetime
import requests
import json
import re
import math
import urllib
import time
import hashlib
import collections
import os
import codecs, markdown

#全局参数区域
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
        "email":email+"@qutoutiao.net"
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
    print(r.content)
    return json.loads(r.content)["retData"]["uid"]



#告警模块
def alert_msg(content,key):
    url_alert = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="
    if None == key or len(key) == 0:
        url_alert=url_alert+"f28eac1c-26da-44e2-84a2-a5b33a93f1ed"
    else:
        url_alert=url_alert+key
    headers={
        'Content-Type':'application/json'
    }
    body = {
        "msgtype": "text",
        "text": {
            "content": "广州今日天气：29度，大部分多云，降雨概率：60%",
            "mentioned_list":["lijixiang@qutoutiao.net","@all"]
        }
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)



if __name__ == "__main__":
    alert_msg("hello","f28eac1c-26da-44e2-84a2-a5b33a93f1ed")
    #print(get_user("lijixiang"))
#{"retCode":0,"retMsg":"成功","retData":{"uid":5388,"userNo":"19043113SH","userName":"lijixiang","password":"$2y$10$6pr.WSHwUSJIdr63nJ4AKeFk/XmaoPwNuYedUAQC/6uSUWx6zxXd.","realName":"李吉祥","rName":"李吉祥","idCardNo":"","mobile":"15055262610","email":"lijixiang@qutoutiao.net","gender":1,"isDefaultPass":1,"addressWork":"上海","state":1,"reportUid":6673,"managerUid":0,"companyId":94,"positionId":3638,"staffType":3,"workType":1,"entryDate":20190401,"fullTimeStartDate":20190630,"tryEndDate":0,"contractStartDate":20190401,"contractEndDate":20220331,"jobTimeStartDate":20190401,"overDirector":0,"isOwner":0,"lastWorkingDay":null,"workWeixinAccount":"lijixiang@qutoutiao.net","adAccount":"lijixiang","didiAccount":"1125903139524912","assginHr":0,"cTime":"2019-03-29 17:37:01","uTime":"2020-06-09 19:15:15","deleteFlag":0,"myLevel":6,"emailName":"lijixiang@qutoutiao.net","positionName":"数据仓库工程师","departmentName":"乐传/中台/数据中心/数据技术与产品部/数仓一组","departmentId":2070,"reportRealName":"刘翀","reportEmail":"liuchong@qutoutiao.net","reportWorkWeixinAccount":"liuchong@qutoutiao.net","avatar":"http://wework.qpic.cn/bizmail/EF0ceHYPuB88RrXYfVKBicYDap2IR77rx1cNJjpFA4I4amNeozNNU4A/0"}}
