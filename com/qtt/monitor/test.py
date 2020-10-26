#-*- coding:utf-8 -*-
#!/usr/bin/python
from scipy.integrate import odeint
import numpy as np
from matplotlib import pyplot as pl

#解决matplotlib显示中文乱码问题
##pl.rcParams['font.sans-serif'] = ['SimHei']
pl.rcParams['axes.unicode_minus'] = False
fig, ax = pl.subplots()

#计算基尼系数
def Gini():
    # 计算数组累计值,从 0 开始
    wealths = [1,2,3]
    cum_wealths = np.cumsum(sorted(np.append(wealths, 0)))
    print("cum_wealths:"+str(cum_wealths)) #累加数组
    # 取最后一个，也就是原数组的和
    sum_wealths = cum_wealths[-1]  ##求和值
    print("sum_wealths:"+str(sum_wealths))
    # 人数的累积占比
    print(np.float(len(cum_wealths) - 1)) ##数组长度-1
    print(np.array(range(0, len(cum_wealths))))
    xarray = np.array(range(0, len(cum_wealths))) / np.float(len(cum_wealths) - 1)
    print("xarray:"+str(xarray))
    # 均衡收入曲线
    upper = xarray
    # 收入累积占比
    yarray = cum_wealths / sum_wealths
    print("yarray:"+str(yarray))
    # 绘制基尼系数对应的洛伦兹曲线
    ax.plot(xarray, yarray)
    ax.plot(xarray, upper)
    ax.set_xlabel(u'人数累积占比')
    ax.set_ylabel(u'收入累积占比')
    pl.show()
    # 计算曲线下面积的通用方法
    B = np.trapz(yarray, x=xarray)
    print("B:"+str(B))
    # 总面积 0.5
    A = 0.5 - B
    print("A+B:"+str(A+B))
    G = A / (A + B)
    return G

a=Gini()
print(a)