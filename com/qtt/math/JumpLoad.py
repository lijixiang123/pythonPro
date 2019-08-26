#coding=utf-8
import hmac, base64, struct, hashlib, time,sys,os,crt


def get_hotp_token(secret, intervals_no):
    key = base64.b32decode(secret, True)
    msg = struct.pack(">Q", intervals_no)
    h = hmac.new(key, msg, hashlib.sha1).digest()
    o = ord(h[19]) & 15
    h = (struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000
    return h

def get_totp_token(secret):
    return get_hotp_token(secret, intervals_no=int(time.time())//30)

if __name__ == "__main__":
    user = "lijixiang"
    gtoken = "C4HLCTXYKFQUCAIS"
    crt.Screen.Send("ssh "+user+"@39.96.159.6 -p 2222 \r")
    result = crt.Screen.WaitForString("password:", 1)
    if result!=0:
        crt.Screen.Send("3er4#ER$ \r")
    result = crt.Screen.WaitForString("[MFA auth]:", 1)
    if result!=0:
        crt.Screen.Send(str(get_totp_token(gtoken))+"\r")
    result = crt.Screen.WaitForString("Opt> ", 1)
    if result!=0:
        crt.Screen.Send("3 \r")
    crt.Screen.Send("3 \r")
    crt.Screen.Send("sudo su - lechuan \r")
    crt.Screen.Send("cd /home/azkaban/ljx \r")