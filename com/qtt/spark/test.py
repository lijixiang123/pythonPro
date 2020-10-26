#!/usr/bin/env python
#coding=utf-8

import requests
import base64
import hashlib
import json

def robot_alert(file):
    f = open(file, "rb")
    base64_data = base64.b64encode(f.read()).decode('utf-8')
    print(base64_data)
    md = hashlib.md5()
    md.update(f.read())
    res1 = md.hexdigest()
    print("=========================\n")
    print(res1)
    url_alert = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=f28eac1c-26da-44e2-84a2-a5b33a93f1ed"
    headers={
        'Content-Type':'application/json'
    }
    body = {
        "msgtype": "image",
        "image": {
            "base64": base64_data,
            "md5": res1
        }
    }
    ret = requests.post(url_alert,headers=headers, data=json.dumps(base64_data))

if __name__ == "__main__":
    robot_alert('/Users/lijixiang/Desktop/abc.png')