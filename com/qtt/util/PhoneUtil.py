#coding=utf-8
import requests
import json

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
    print '============ WECHAT ALERT =========='
    print ret.content


if __name__ == "__main__":
    phone_alert("数据有问题需要处理","15055262610")