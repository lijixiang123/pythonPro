#coding=utf-8
import requests
import json

#电话
def get_data(sql):
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
        "sql":sql
    }
    ret = requests.post(url_alert, data=json.dumps(body),headers=headers)
    print '============ WECHAT ALERT =========='
    print ret.content
    ret.content


if __name__ == "__main__":
    get_data("select * from qttdm.srpt_corekpi_kdd_di where dt= '2019-07-23'")