#coding=utf-8
import requests
import json
import datetime


#企业微信
def qw_alert(app_name):
    url_alert = "http://data.qutoutiao.net/message/api/alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uids":"5388",
        "types":"2",
        "subject":"核心管报任务",
        "content":"核心管报任务有问题，请及时处理。"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print '============ WECHAT ALERT =========='
    print ret.content

def alert(subject, content):
    now_minutes = datetime.now().strftime("%M")
    now_minutes = int(now_minutes)

    url_alert = "http://data.qutoutiao.net/message/api/alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uids":"",
        "types":"2",
        "subject":subject,
        "content":content
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')