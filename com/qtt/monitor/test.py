#coding=utf-8
import sys
import datetime
import requests
import json
import re
import math


#告警模块
def alert_msg(uids,types,subject,content):
    url_alert = "http://data.qutoutiao.net/message/api/alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uids":uids,  #语音播报内容
        "types":types,
        "subject":subject,
        "content":content
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)


#电话
def phone_alert(content):
    url_alert = "http://data.qutoutiao.net/message/api/phone_alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "content":content,  #语音播报内容
        "userName":"李吉祥"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)

#企业微信
def qw_alert(msg):
    url_alert = "http://data.qutoutiao.net/message/api/alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uids":"5388",
        "types":"2",
        "subject":"核心管报任务",
        "content":msg
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print(ret.content)


#企业微信
def get_user(uid):
    url_alert = "https://oa.qutoutiao.net/api/organization/user/report-relation"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uid":uid
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print(ret.content)
    data=json.loads(ret.content)["retData"]["child"]
    print("=========")
    print data

#雨燕平台企业微信群通知
def qun_alert(content,subject):
    url_alert = "http://data.qutoutiao.net/message/api/weixin_group_alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "content":content,  #语音播报内容
        "subject":subject,
        "senderEmail":"liuchao02@qutoutiao.net",
        "weixinGroupId":"5d5cb0558931853575408819"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)


if __name__ == "__main__":
    str_list='23338888888888888234.0000000'
    print(str_list)

    if str(str_list).count('.') > 0 :
        print(str(str_list)[0:str(str_list).index('.',0,len(str(str_list)))+7])
    else:
        print(str_list)
