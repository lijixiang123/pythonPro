#coding=gbk
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
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
#import mysql.connector


#全局参数区域
url = "https://oa.qutoutiao.net/api/open-api/get-user-by-email"
HEADERS = {'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'}
appSecret = '7cb959f34d53396f7e0f4156cb6a46df'
appId = 'DQC'
timestamp = int(time.time())
alert_type_tuple={"mail":"1","email":"1","wx":"2","weixin":"2","phone":"3"}

#mysql链接
conf_mysql = {
    'host': 'rm-2zezs8cj37o38v9s4.mysql.rds.aliyuncs.com',
    'user': 'airflow_r',
    'password': 'BZ!ju7MleEP&Lho@',
    'port': 3306,
    'database': 'airflow',
}

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

#雨燕平台企业微信群通知
def qun_alert(subject,content):
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
    url_alert = "http://test-data.qttcs3.cn/newqe/api/job/execute"
    headers={
        'Content-Type':'application/json',
        'Connection':'close'
    }
    body = {
        "engine":engine,
        "taskName":"test",
        "userName":"lijixiang",
        "user":"default",
        "sql":"show databases"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    data=json.loads(ret.content)
    print(str(data))
    return data


#api查询数据，并返回
def get_data(v_sql,engine):
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
        "sql":"show databases"
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    data=json.loads(ret.content)
    print(str(data))
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

#区间判断
def check_data(data,cols):
    msg=""
    col={}
    datas=data["data"]
    columns=data["columnsName"]
    cols_cell=re.split(";",cols)
    for c in cols_cell:
        k=re.split(":",c)[0]
        v=re.split(":",c)[1]
        print(k)
        print(v)
        col[k]=v

    #获取没行的数据
    for row_data in datas:
        #列位置
        i=0
        #获取每列的值
        while i < len(row_data):
            c_min=col[columns[i]].split(",")[0]
            c_max=col[columns[i]].split(",")[1]
            if row_data[i] is not None and str(row_data[i]).strip() != "" and str(row_data[i]).strip() != "NULL" and str(row_data[i]).strip() != "null":
                if float(row_data[i]) == 0:
                    #总异常记录数
                    if str(row_data[i]).count('.') > 0:
                        msg=msg+columns[i]+"当前值:0"+":区间范围["+str(c_min)[0:str(c_min).index('.',0,len(str(c_min)))+7]+","+str(c_max)[0:str(c_max).index('.',0,len(str(c_max)))+7]+"]\n"
                    else:
                        msg=msg+columns[i]+"当前值:0"+":区间范围["+str(c_min)+","+str(c_max)+"]\n"

                elif ((float(row_data[i]) > float(c_max)) or (float(row_data[i]) < float(c_min))):
                    #总异常记录数
                    if str(row_data[i]).count('.') > 0:
                        msg=msg+columns[i]+"当前值:"+str(row_data[i])[0:str(row_data[i]).index('.',0,len(str(row_data[i])))+7]+":区间范围["+str(c_min)[0:str(c_min).index('.',0,len(str(c_min)))+7]+","+str(c_max)[0:str(c_max).index('.',0,len(str(c_max)))+7]+"]\n"
                    else:
                        msg=msg+columns[i]+"当前值:"+str(row_data[i])+":区间范围["+str(c_min)+","+str(c_max)+"]\n"

            else:
                msg=msg+columns[i]+"当前值:NULL"+":区间范围["+str(c_min)+","+str(c_max)+"]\n"

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
def handle_msg2(result_msg,dag_id,task_id,owners,filename,content):
    ret_msg='\n'
    return ret_msg \
           +"task任务:"+str(dag_id+"."+task_id) +"\n" \
           +"负责人:"+str(owners) +"\n" \
           +"监测hql名称:"+str(filename) +"\n" \
           +"明细结果:"+"\n"+str(result_msg) +"\n" \
           +"监测hql内容:"+"\n"+str(content)[0:500]

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
def get_uids(users):
    uids=''
    us=re.split("[,;；|]",users)
    for name in us:
        uids=uids+str(get_user(name))+","
    return uids[:-1]

#查询hql文件描述
def get_meta_data(query):
    #con = mysql.connector.connect(**conf_mysql)
    #cursor = con.cursor()
    #cursor.execute(query)
    #rows = cursor.fetchall()
    #cursor.close()
    #con.close()
    return ""

#mail,wx,phone,返回数字
def get_alert_type(alert_types):
    alerttype=''
    alerts=re.split("[,;；|]",alert_types.lower())
    for alert in alerts:
        alerttype=alerttype+alert_type_tuple[alert]+","
    return alerttype[:-1]


if __name__ == "__main__":
    #入参日期，例如：2019-01-01
    date_time = "2019-08-01"
    #hive的数据源，暂时默认的，参数没有使用
    data_source = "clickhouse"
    #hql文件的绝对路径位置
    file_name = "/home/ljixiang/aa.hql"
    #c1:1.2,5;c2:3,4验证规格，先;切割，在:切分，在,切分
    cols = 'dau:-1,33115320;loss_tuid_cnt:6542531,7424819;lost_ratio:0.1551590825698193,0.19509734052514274;reflux_tuid_cnt:4203214,5547517;reg_7d_dau:28101681,30759757;reg_7d_reflux:4044328,5230671;reg_7d_reflux_ratio:0.13721860563550323,0.1654054969966022;reg_7d_loss:4092464,5306020;reg_7d_loss_ratio:0.13965574682093937,0.160724181721558;reflux_ratio:0.14279400829951913,0.17684550051949974;net_loss_ratio:0.002973022359233894,0.033781486826944936;net_loss_7d_ratio:-0.009065493301360307,0.014635221485656857;nlx_tuid_cnt:81543,168474;wlx_tuid_cnt:292713,859284;nlx_remain_1:0.5280603271384182,0.6037417255007526;wlx_remain_1:0.2961009907318632,0.39277748688827624;coin_wlx_tuid_cnt:0,771860;content_wlx_tuid_cnt:0,104237;coin_wlx_remain_1:0.3080705183874153,0.4073393739311803;content_wlx_remain_1:0.19758957654723128,0.25430235636748744;remain_1_ratio:0.8451985067701275,0.8690863225466637;remain_1_60d_tuid_cnt:21061136,21641270;reg_60d_tuid_cnt:24542132,25527215;year_week_remain_ratio:0.11171786217038705,0.11783247820550866;year_week_remain_mol:16632014,18534249;year_week_remain_den:141149658,163487697;new_remain_7:0.19371860406191668,0.23372102726870259;nlx_remain_7:0.3213653391691553,0.38691007867656957;coin_wlx_remain_7:0.16646943967812555,0.21433800553522006;content_wlx_remain_7:0.08867893793777488,0.11157343556417869;sx_nlx_tuid_num:10,30000'
    #告警类型1,2,3
    alert_type = "wx"
    users = "lijixiang"

    #默认引擎presto
    engine='presto'
    #默认presto抽样1000条
    data_count=1
    #异常占比
    err_data_rate=0.1

    #参数准备
    #fp=open(file_name,"r")
    #curr_day=(datetime.datetime.strptime(date_time, "%Y-%m-%d")-datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    #bef1_day=(datetime.datetime.strptime(date_time, "%Y-%m-%d")-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    #content=open(file_name,"r").read().replace("${YESTERDAY}",date_time).replace("${YESTERDAY1}",bef1_day).replace("${TODAY}",curr_day)
    v_sql = "show databases;"

    #带有列名和数据的列表
    #data=get_data(v_sql,engine)
    data={u'status': 200, u'errorCode': 0, u'message': u'ok', u'columnsName': [u'dau', u'loss_tuid_cnt', u'lost_ratio', u'reflux_tuid_cnt', u'reg_7d_dau', u'reg_7d_reflux', u'reg_7d_reflux_ratio', u'reg_7d_loss', u'reg_7d_loss_ratio', u'reflux_ratio', u'net_loss_ratio', u'net_loss_7d_ratio', u'nlx_tuid_cnt', u'wlx_tuid_cnt', u'nlx_remain_1', u'wlx_remain_1', u'coin_wlx_tuid_cnt', u'content_wlx_tuid_cnt', u'coin_wlx_remain_1', u'content_wlx_remain_1', u'remain_1_ratio', u'remain_1_60d_tuid_cnt', u'reg_60d_tuid_cnt', u'year_week_remain_ratio', u'year_week_remain_mol', u'year_week_remain_den', u'new_remain_7', u'nlx_remain_7', u'coin_wlx_remain_7', u'content_wlx_remain_7', u'sx_nlx_tuid_num'], u'data': [[u'0', u'6164430', u'0.1873847944320322', u'5547517', u'30759757', u'5230297', u'0.15898925418302662', u'5282089', u'0.16056361438716937', u'0.16863202804690847', u'0.018752766385123732', u'0.0016837584250096644', u'135991', u'603727', u'0.5474963629718282', u'0.36559177351547123', u'576056', u'27671', u'0.3718915718253934', u'0.23562789927104041', u'0.8513437862235682', u'21523133', u'25281365', u'0.11171786217038705', u'18264496', u'163487697', u'0.2005524823126166', u'0.33317341590887617', u'0.17888877711833148', u'0.0963105276265699', u'NULL']]}
    if len(data["data"]) == 0:
        msg="没有数据"
        #exit(1)

    #获取实际记录数
    data_count=get_min(data_count,len(data["data"]))

    #获取逻辑判断后的信息
    if len(data["data"]) > 0:
        msg=check_data(data,cols)

    #获取uid列表
    uids=get_uids(users)

    #文件名截取
    filename=os.path.basename(file_name)

    #hql文件desc描述获取
    query_hql_desc = "abc"
    hql_desc="desc"

    #获取task_id
    query_task_id = "abcdef"
    dag_id="ab"
    task_id="def"
    owners="lijixiang"

    #报警类型处理
    alert_types=get_alert_type(alert_type)

    #最后模版消息
    result_msg=handle_msg2(msg,dag_id,task_id,owners,filename,"abc")

    #超过样本0.1告警
    #if float(msg['total']) >= float(math.ceil(data_count*err_data_rate)):
    if len(msg) > 0:
        #异常结果显示
        print()
        #异常偏大，告警，并异常退出
        alert_msg(uids,get_alert_type(alert_type),"数据质量抽查"+filename,handle_msg(msg))
        #alert_msg(uids,alert_types,hql_desc,result_msg)
        #ETL监控群
        #qun_alert(hql_desc,result_msg)
        #exit(1)
    else:
        print(filename+"文件规则检查通过")

    #关闭文件流
    #fp.close()