#coding:utf-8
import crt

# $language = "Python"
# $interface = "1.0"

#crt.Dialog.FileOpenDialog([title,[buttonLabel,[defaultFilename,[filter]]]])
#弹出一个对话框，用于选择单个文件;如果选择了具体文件则返回该文件的绝对路径，如果选择了弹窗的“取消”，则返回空。
filePath =  crt.Dialog.FileOpenDialog("please open a file","open","a.log","(*.log)|*.log")
#filePath =  crt.Dialog.FileOpenDialog("","","a.log","")
#crt.Dialog.MessageBox(message, [title, [icon|buttons]]) 警告、按钮类型弹出一个消息框，可以定义按钮，使用按钮和文本消息来实现和用户的简单对话；
crt.Dialog.MessageBox(filePath,"",64|0)
crt.Dialog.MessageBox("会话已断开","session",64|2)
crt.Dialog.MessageBox("确认是否退出","session",32|1)
crt.Dialog.MessageBox("确认是否退出","session",32|3)
crt.Dialog.MessageBox("是否继续安装","session",32|4)
crt.Dialog.MessageBox("此会话已打开","session",48|5)
crt.Dialog.MessageBox("无法连接此窗口","session",16|6)

#crt.Dialog.Prompt(message [, title [,default [,isPassword ]]])
#弹出一个输入框，用户可以填写文字，比如填写文件名，填写路径，填写IP地址等,运行结果如果点击'ok'，返回输入的字符串，否则返回""
password = crt.Dialog.Prompt("password","session","admin",False)
crt.Dialog.MessageBox(password,"password",64|0)
password = crt.Dialog.Prompt("password","session","",True)
crt.Dialog.MessageBox(password,"password",64|0)