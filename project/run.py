#!/usr/bin/env python
#  -*- coding: utf-8 -*-

import logging
import os
import pickle
import tushare
import time
import types
import random
import datetime


# from proxyfetcher import ProxyFetcher


class TS():
    def __init__(self):
        self.cache_d = {}

        def __getattribute__(self, item):
            print("use getattribute")
            x = super().__getattribute__(
                item)  # 重点：如果父类调用找不到会直接在父类那里面调用__getattr__，如果还没找到就会直接返回了，这一行下面的代码不会执行。这里有点扯，我觉得是python的bug。。。
            print(isinstance(x, classmethod))
            print(type(x))
            print(str(type(x)) == "<class 'method'>")  # 判断是否为类的方法属性或函数，具体分析请看下面
            print(item + ":" + str(x))
            return x

    def md5_str(self, *args):
        import hashlib
        m = hashlib.md5()
        m.update(args.__str__().encode())
        return m.hexdigest()

    def md5_path(self, md5):
        return '{}{}{}'.format(os.sep.join(f'{ch[0]}{ch[1]}' for ch in zip(md5[::2], md5[1::2])), os.sep, md5)

    def cache_run(self, func, args, kwargs, cache_file='cache.pkl',cache_fold='cache'):
        fold_path = f'{cache_fold}/{func.__name__}/{self.md5_path(self.md5_str(args, kwargs))}'
        file_path = f'{fold_path}/{cache_file}'
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                try:
                    self.cache_d = pickle.load(f)
                except EOFError:
                    pass
        key = func.__name__ + args.__str__() + kwargs.__str__()
        if key in self.cache_d:
            print('读取缓存')
            res = self.cache_d[key]
        else:
            res = func(*args, **kwargs)
            self.cache_d[key] = res
            if not os.path.exists(fold_path):
                os.makedirs(fold_path)
            with open(file_path, 'wb') as wf:
                pickle.dump(self.cache_d, wf)
        return res

    def cache_run_backup(self, func, args, kwargs, cache_file='cache.pkl',cache_fold='cache'):
        fold_path = f'{cache_fold}/{func.__name__}/{self.md5_path(self.md5_str(args, kwargs))}'
        file_path = f'{fold_path}/{cache_file}'
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                try:
                    self.cache_d = pickle.load(f)
                except EOFError:
                    pass
        key = func.__name__ + args.__str__() + kwargs.__str__()
        if key in self.cache_d:
            print('读取缓存')
            res = self.cache_d[key]
        else:
            res = func(*args, **kwargs)
            self.cache_d[key] = res
            if not os.path.exists(fold_path):
                os.makedirs(fold_path)
            with open(file_path, 'wb') as wf:
                pickle.dump(self.cache_d, wf)
        return res

    def cache_run(self, func, args, kwargs, cache_file='cache.pkl',cache_fold='cache'):
        fold_path = f'{cache_fold}/{func.__name__}/{self.md5_path(self.md5_str(args, kwargs))}'
        file_path = f'{fold_path}/{cache_file}'
        run_flag = False
        if os.path.exists(file_path):
            print('读取缓存')
            with open(file_path, 'rb') as f:
                try:
                    res = pickle.load(f)
                except EOFError:
                    run_flag = True
                    res = None
                    pass
        else:
            run_flag = True
            res = None
        if run_flag:
            res = func(*args, **kwargs)
            if not os.path.exists(fold_path):
                os.makedirs(fold_path)
            with open(file_path, 'wb') as wf:
                pickle.dump(res, wf)
        return res

def get_ping_url():
    return 'http://baidu.com'
    # return 'https://2.taobao.com'
    # return 'https://2.taobao.com/app/index?spm=2{}7.1000261.0.0.135334f1bgKQqG'.format(random.randint(10, 99))

import sys
from inspect import isfunction

mod = tushare
ts = tushare

cache_d = TS()
def timer(func):
    # print("function %s" % (func))

    def wrapper(*args, **kwargs):
        start = time.time()
        print("before %s called [%s]." % (func.__name__, args))
        global cache_d
        res = cache_d.cache_run(func, args=args, kwargs=kwargs)
        print("function %s run time %s" % (func.__name__, time.time() - start))
        return res

    return wrapper


def deco(*args):
    def _deco(func):
        def __deco():
            print("before %s called [%s]." % (func.__name__, args))
            func()
            print("  after %s called [%s]." % (func.__name__, args))

        return __deco

    # 当直接使用 @deco 定义的时候第一个参数为函数
    if len(args) == 1 and type(args[0]) is types.FunctionType:
        return _deco(args[0])

    return _deco

for func in dir(mod):
    try:
        # print(func)
        if func != 'timer' and isfunction(eval(f'{mod.__name__}.{func}')) and func != 'isfunction':
            f = getattr(sys.modules[mod.__name__], func, None)
            if f:
                setattr(sys.modules[mod.__name__], func, timer(f))
    except (TypeError, NameError) as e:
        print(e)



def run_chao_e_shou_yi(piao, name):
    # cpi = ts.get_cpi()
    # print(cpi)
    print(piao)
    # start = '2020-03-15'
    # end = (datetime.datetime.fromisoformat(start)+datetime.timedelta(days=31)).isoformat().split('T')[0]
    end = (datetime.date.today().isoformat()).split('T')[0]
    start = (datetime.datetime.fromisoformat(end)-datetime.timedelta(days=21)).isoformat().split('T')[0]
    print(start, '=======>',end)
    # piao = "002071"
    # 获取沪深300历史数据
    hs300 = tushare.get_k_data("hs300", start=start, end=end)
    # print(hs300)
    jl = tushare.get_k_data(piao, start=start, end=end)
    print(jl)
    # 计算 HPR 函数
    def hpr(endPrice, periodPrice):
        # print(endPrice)
        endPrice = float(endPrice)
        periodPrice = float(periodPrice)
        return (endPrice - periodPrice) / periodPrice

    # print(dir(jl))
    all_c = 0
    last_jlr = None
    # print(jl.to_json())
    if len(jl) == 0:
        return 0, name
    for jlr in jl["date"]:
        # print(jlr)
        day_str =jlr
        # print(day_str)
        if last_jlr:
            day_str_last = last_jlr

            hpr_yearly = hpr(hs300[hs300["date"] == day_str]["close"], hs300[hs300["date"] == day_str_last]["close"])
            jl_yearly = hpr(jl[jl["date"] == day_str]["close"], jl[jl["date"] == day_str_last]["close"])
            # print('沪深300持有期收益率 HPR', hpr_yearly)
            # print('金龙机电持有期收益率 HPR', jl_yearly)
            # print('超额收益率', jl_yearly - hpr_yearly)
            all_c += (jl_yearly - hpr_yearly)
        last_jlr = jlr

    # for day_str in [
    #     ((datetime.datetime.fromisoformat('2017-10-09')+datetime.timedelta(days=i)).isoformat().split('T')[0] ,
    #     (datetime.datetime.fromisoformat('2017-10-09')+datetime.timedelta(days=i+1)).isoformat().split('T')[0]) for i in range(30)
    # ]:
    #     print(day_str)
    #
    #     try:
    #         hpr_yearly = hpr(hs300[hs300["date"] == day_str[0]]["close"], hs300[hs300["date"] == day_str[1]]["close"])
    #         jl_yearly = hpr(jl[jl["date"] == day_str[0]]["close"], jl[jl["date"] == day_str[1]]["close"])
    #         print('沪深300持有期收益率 HPR',hpr_yearly)
    #         print('金龙机电持有期收益率 HPR',jl_yearly)
    #         print('超额收益率',jl_yearly-hpr_yearly)
    #         all_c += (jl_yearly-hpr_yearly)
    #     except Exception as e:
    #         print(e)
    print("\n\n%s -> %s (%s) %s 累积超额收益率：%.2f%%" % (start, end, piao, name, all_c*100))
    return all_c, name
def run():
    """
    ConnectTimeout
    指的是建立连接所用的时间，适用于网络状况正常的情况下，两端连接所用的时间。
    在java中，网络状况正常的情况下，例如使用HttpClient或者HttpURLConnetion连接时设置参数connectTimeout=5000即5秒，
    如果连接用时超过5秒就是抛出java.net.SocketException: connetct time out的异常。
    ReadTimeout
    指的是建立连接后从服务器读取到可用资源所用的时间。
    在这里我们可以这样理解ReadTimeout：正常情况下，当我们发出请求时可以收到请求的结果，也就是页面上展示的内容，但是当网络状况很差的时候，
    就会出现页面上无法展示出内容的情况。另外当我们使用爬虫或者其他全自动的程序时，无法判断当前的网络状况是否良好，此时就有了ReadTimeout的
    用武之地了，通过设置ReadTimeout参数，例：ReadTimeout=5000，超过5秒没有读取到内容时，就认为此次读取不到内容并抛出
    Java.net.SocketException: read time out的异常。
    """

    logging_level = logging.INFO

    logging.basicConfig(
        level=logging_level,
        format=('%(asctime)s.%(msecs)03d [%(levelname)s] '
                '[%(process)d:%(threadName)s:%(funcName)s:%(lineno)d] %(message)s'),
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # proxy = ProxyFetcher('socks5h://127.0.0.1:1887')
    # while True:
    #     if not proxy.alive(get_ping_url()):
    #         proxy.fetch_proxy_ip(True)
    #         continue
    #     logging.info('Succeed to start localSocks (ip {}, startTime {})'.format(proxy.ip, proxy.lasttime))
    #     time.sleep(10 + random.randint(1, 40))

def run_all_chaoe():
    r = ts.get_stock_basics()['name'].to_dict()
    max_r = 0
    max_n = ''
    all_r = []
    for idx, kv in enumerate(r.items()):

        res = run_chao_e_shou_yi(kv[0], kv[1])
        all_r.append({'name':res[1], 'v':res[0]})
        max_r = max(max_r, res[0])
        if max_r == res[0]:
            max_n = res[1]
        print(idx, len(r), max_n, '%.2f%%' % (max_r * 100))
    all_r.sort(key=lambda x: x['v'])
    print(all_r)
    with open(f'res-gupiao{datetime.date.today()}.txt', 'w', encoding='utf-8') as f:
        for line in all_r:
            f.write(f'{line}\n')

if __name__ == '__main__':
    # run()
    run_all_chaoe()
    # run_chao_e_shou_yi('300032', '金龙机电')

