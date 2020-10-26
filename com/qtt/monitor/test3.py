#coding=utf-8
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



#全局参数区域
url = "https://oa.qutoutiao.net/api/open-api/get-user-by-email"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
appSecret = '7cb959f34d53396f7e0f4156cb6a46df'
appId = 'DQC'
timestamp = int(time.time())
alert_type_wx={"mail":"1","email":"1","wx":"2","weixin":"2"}
alert_type_phone={"phone":"3"}
dag_type_tuple={"presto":"dq_day_check","clickhouse":"dq_minute_check"}

#mysql链接
conf_mysql = {
    'host': 'rm-2zezs8cj37o38v9s4.mysql.rds.aliyuncs.com',
    'user': 'airflow_r',
    'password': 'BZ!ju7MleEP&Lho@',
    'port': 3306,
    'database': 'airflow',
}

#日志数据写入mysql
log_mysql = {
    'host': 'rm-2zespg861xcsdhjn1.mysql.rds.aliyuncs.com',
    'user': 'dwdata_w',
    'password': 'Wz729D4753q3456g',
    'port': 3306,
    'database': 'dwdata',
}

#告警模块
def alert_msg(uids,types,subject,content):
    print("uids:"+str(uids))
    print("types:"+str(types))
    print("subject:"+str(subject))
    print("content:"+str(content))
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


#雨燕平台企业微信群通知
def qun_alert(sender,groupid,subject,content):
    url_alert = "http://data.qutoutiao.net/message/api/weixin_group_alert"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "content":content,  #语音播报内容
        "subject":subject,
        "senderEmail":sender,
        "weixinGroupId":groupid
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)

#通过人员名称查找uid，目前无接口可用
def get_uids(names):
    url_alert = "https://oa.qutoutiao.net/api/open-api/get-user-by-email"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "email":names
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print('============ WECHAT ALERT ==========')
    print(ret.content)

#api查询数据，并返回
def get_data(v_sql,engine):
    retry_cnt=3
    url_alert = "http://inner-query-editor.1sapp.com/api/job/execute"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "engine":engine,
        "taskName":"test",
        "userName":"lijixiang",
        "user":"default",
        "sql":v_sql
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    data=json.loads(ret.content)
    errCode=int(data["errorCode"])
    if errCode==1000:
        print("presto查询连接池满，返回错误码1000，尝试3次重试查询")
        while(retry_cnt > 0 and errCode == 1000):
            ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
            data=json.loads(ret.content)
            errCode=int(data["errorCode"])
            retry_cnt=retry_cnt-1
    return data

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

#小数点处理保留6位
def get_num(num):
    if num.count('.') > 0:
        return str(num)[0:str(num).index('.',0,len(str(num)))+7]
    else:
        return str(num)

#区间判断
def check_data(data,cols):
    msg=""
    col={}
    datas=data["data"]
    columns=data["columnsName"]
    print(columns)
    cols_cell=re.split(";",cols)
    for c in cols_cell:
        k=str(re.split(":",c)[0]).decode("utf-8")
        v=re.split(":",c)[1]
        col[k]=v
    print("=====col======="+str(col))
    #获取没行的数据
    for row_data in datas:
        #列位置
        i=0
        #获取每列的值
        while i < len(row_data):
            col_i=str(columns[i]).decode("utf-8")
            print("col_i:=="+col_i)
            c_min=col[col_i].split(",")[0]
            c_max=col[col_i].split(",")[1]
            if row_data[i] is not None and str(row_data[i]).strip() != "" and str(row_data[i]).strip() != "NULL" and str(row_data[i]).strip() != "null":
                if float(row_data[i]) == 0:
                    msg=msg+col_i+"当前值:0"+":区间范围["+get_num(c_min)+","+get_num(c_max)+"]\n"

                elif ((float(row_data[i]) > float(c_max)) or (float(row_data[i]) < float(c_min))):
                    msg=msg+col_i+"当前值:"+get_num(row_data[i])+":区间范围["+get_num(c_min)+","+get_num(c_max)+"]\n"
            else:
                msg=msg+col_i+"当前值:NULL"+":区间范围["+get_num(c_min)+","+get_num(c_max)+"]\n"
            i=i+1
    return msg[:-1]

#取最小值
def get_min(a,b):
    if a > b:
        return b
    else:
        return a

#取最大值
def get_max(a,b):
    if a > b:
        return a
    else:
        return b

#语音对特殊字符无法读出，处理
def handle_msg(msg):
    ret_msg=''
    for i in msg.keys():
        ret_msg=ret_msg+str(i)+"_err_"+str(msg[i])+","
    if len(ret_msg)>0:
        return ret_msg[:-1]

#模版
def handle_msg2(result_msg,dag_id,task_id,owners,filename,content,msg_template):
    ret_msg='\n'
    if msg_template is None or len(msg_template) == 0 or msg_template == "all":
        ret_msg=ret_msg \
                +"task任务:"+str(dag_id+"."+task_id) +"\n" \
                +"负责人:"+str(owners) +"\n" \
                +"监测hql名称:"+str(filename) +"\n" \
                +"明细结果:"+"\n"+str(result_msg) +"\n" \
                +"监测hql内容:"+"\n"+str(content)[0:500]
    else:
        ms=re.split(",",msg_template)
        for m in ms:
            if m=="task_id":
                ret_msg=ret_msg + "task任务:"+str(dag_id+"."+task_id) +"\n"
            elif m=="owners":
                ret_msg=ret_msg + "负责人:"+str(owners) +"\n"
            elif m=="filename":
                ret_msg=ret_msg +"监测hql名称:"+str(filename) +"\n"
            elif m=="result_msg":
                ret_msg=ret_msg +"明细结果:"+"\n"+str(result_msg) +"\n"
            elif m=="content":
                ret_msg=ret_msg +"监测hql内容:"+"\n"+str(content)[0:500] + "\n"

    return ret_msg[:-1]

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
    return json.loads(r.content)["retData"]["uid"]

#通过签名获取人员名称返回uids信息
def get_uids(users,alert_type):
    uids=''
    us=re.split("[,;；]",users)
    for name in us:
        col_name=re.findall(re.compile(r'^[a-zA-Z]\w*',re.S),name)
        col_alert=re.findall(re.compile(r'[(](.*?)[)]', re.S),name)
        if len(col_name) > 0 and name.count("(") == 0 and name.count(")")==0 and name.count("|") ==0:
            uids=uids+str(get_user(col_name[0]))+"|"+alert_type+"_"
        elif len(col_name) > 0 and name.count("(") > 0 and name.count(")")>0:
            uids=uids+str(get_user(col_name[0]))+"|"+alert_type+","+str(col_alert[0]).replace("|",",")+"_"
    return uids[:-1]

#获取群组列表
def get_phones(users):
    uids=''
    us=re.split("[,;；]",users)
    for name in us:
        if len(name) == 11 and re.match(r"^1[35678]\d{9}$",name) :
            uids=uids+str(name)+","
    return uids[:-1]


#获取群组列表
def get_quns(users):
    uids=''
    us=re.split("[,;；]",users)
    for name in us:
        if name.count("|") >  0 and name.count("(") <= 0 and name.count(")") <= 0 :
            uids=uids+str(name.split("|")[0])+"@qutoutiao.net|"+str(name.split("|")[1])+","
    return uids[:-1]

#获取群组列表
def get_quns(users):
    uids=''
    us=re.split("[,;；]",users)
    for name in us:
        if name.count("|") >  0 :
            uids=uids+str(name.split("|")[0])+"@qutoutiao.net|"+str(name.split("|")[1])+","
    return uids[:-1]


#mail,wx返回数字
def get_alert_wx(alert_types):
    alerttype=''
    alerts=re.split("[,;；|]",alert_types.lower())
    for alert in alerts:
        if alert != "phone":
            alerttype=alerttype+alert_type_wx[alert]+","
    return alerttype[:-1]

#phone返回数字
def get_alert_phone(alert_types):
    alerttype=''
    alerts=re.split("[,;；|]",alert_types.lower())
    for alert in alerts:
        if alert == "phone":
            alerttype=alerttype+alert_type_phone[alert]+","
    return alerttype[:-1]

def get_quns(users):
    uids=''
    us=re.split("[,;；]",users)
    for name in us:
        if name.count("|") >  0 :
            uids=uids+str(name.split("|")[0])+"@qutoutiao.net|"+str(name.split("|")[1])+","
    return uids[:-1]

def get_robots(users):
    robots=''
    us=re.split("[,;；]",users)
    for name in us:
        if re.findall(r'\w*-\w*-\w*-\w*-\w*', name) :
            robots=robots+name+","
    return robots[:-1]
#机器人
def robot_alert(content,users):
    url_alert = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=f28eac1c-26da-44e2-84a2-a5b33a93f1ed"
    headers={
        'Content-Type':'application/json'
    }
    body = {
        "msgtype": "text",
        "text": {
            "content": content,
            "mentioned_mobile_list":[users]
        }
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)

if __name__ == "__main__":

        #获取群组列表
    robot_alert("f28eac1c-26da-44e2-84a2-a5b33a93f1ed",'15055262610')
    #print(get_user("guhongcheng"))
    #5388,7935
    #alert_msg("5388","2","检测算法t+2数据是否都已准备_固定推送","测试")
