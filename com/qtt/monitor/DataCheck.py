#coding=utf-8
import sys
import datetime
import requests
import json
import re

#统一告警入口
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
def phone_alert(content,phone):
    url_alert = "http://data.qutoutiao.net/message/api/phone_alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "content":content,  #语音播报内容
        "phoneNum":phone,
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
    print('============ WECHAT ALERT ==========')
    print(ret.content)

#通过人员名称查找uid，目前无接口可用
def get_uid(msg):
    url_alert = "https://oa.qutoutiao.net/api/organization/user/report-relation-tree"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "uid":"1085"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)



def get_data(v_sql):
    url_alert = "http://inner-query-editor.1sapp.com/api/job/execute"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "engine":"presto",
        "taskName":"test",
        "userName":"lijixiang",
        "user":"default",
        "sql":v_sql
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    data=json.loads(ret.content)
    return data

def get_columns(data):
    return data['columnsName']


def get_row(data):
    length=len(data['data'])
    if 0 == length:
        return 0
    else:
        return data['data'][0]

def get_col_count(data):
    return len(data['columnsName'])

#非空判断
def is_null(data):
    if 0==len(data['data']):
        return 1
    else:
        return 0

#为0判断
def is_zero(data):
    if data == 0 or data == 0.0 or data is None:
        return 0
    else:
        return data

def get_Msg(columns,rows,count):
    i = 0
    msg=""
    while ( i < count):
        column = columns[i]
        row = rows[i]
        flag = is_zero(row)
        if flag == "0" or flag == "0.0":
            print(msg)
            print(column)
            msg=msg+column+"_0,"
        i=i+1
    print(msg)
    print(msg[:-1])
    return msg



if __name__ == "__main__":
    path = sys.argv[1]
    phone="15055262610"
    fp = open(path,"r")
    text = fp.readline()

    msg=""
    while text:
        content=re.split('[,:;|]',text)[1]
        day_time=re.split('[,:;|]',text)[0]
        #参数定义
        dt=(datetime.date.today()-datetime.timedelta(days=int(day_time))).strftime("%Y-%m-%d")
        v_sql="select * from "+content+" where dt = '"+dt+"'"
        data = get_data(v_sql)
        null_flag = is_null(data)
        if 1==null_flag:
            msg=msg+content+"_no_data,"
        else:
            columns = get_columns(data)
            rows = get_row(data)
            count = get_col_count(data)
            content_msg=get_Msg(columns,rows,count)
            if content_msg is not None and len(content_msg) > 0 :
                msg = msg+content+"_"+content_msg
        text = fp.readline()
    #消息处理
    if msg is not None and len(msg) > 0 :
        qw_alert(msg)
        phone_alert(msg,phone)
        phone_alert(msg,phone)
    fp.close()