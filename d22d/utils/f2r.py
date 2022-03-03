import re
import json
import sys
import time
import re
import urllib.parse
import clipboard


def fetch_to_requests(fetch):
    raw = fetch.strip().split('fetch(', 1)[-1][:-2]
    url, data = re.fullmatch(r'"(.*)", {(.*)', raw, re.DOTALL).groups()
    url_d = urllib.parse.urlsplit(url)
    print(url_d.netloc)
    print(url_d.fragment)
    print(url_d.scheme)
    print(url_d.hostname)
    url_format = "{}_{}".format(
        url_d.hostname.replace('.', '_'),
        re.sub(r'[^a-zA-Z0-9]', '_', url_d.path)
    )
    print(url_format)
    data = json.loads(f'{{{data}')

    headers = data['headers']
    headers["Host"] = url_d.netloc
    headers["Referer"] = data.get('referrer') or url
    headers["Origin"] = headers["Referer"][:-1] if headers["Referer"].endswith('/') else headers["Referer"]
    headers["Connection"] = "keep-alive"
    for k in [
        "Sec-Fetch-Dest",
        "Sec-Fetch-Mode",
        "Sec-Fetch-Site",
    ]:
        if (kl := k.lower()) in headers:
            headers[k] = headers.pop(kl)
    # headers["Sec-Fetch-Dest"] = "script",
    # headers["Sec-Fetch-Mode"] = "no-cors",
    # headers["Sec-Fetch-Site"] = "same-origin",
    # headers["Accept-Encoding"] =  "gzip, deflate, br"
    # headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,i"
    # "mage/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    # headers["Accept-Language"] = "zh,zh-CN;q=0.9,en;q=0.8",
    headers_str = '{{\n        "User-Agent": get_ua(), {}    }}'.format(json.dumps(
        headers, separators=(",", ": "), indent=8)[1:-1])
    proxies_host = '127.0.0.1'
    proxies_port = 8389
    proxies = {
        "http": f"socks5://{proxies_host}:{proxies_port}",
        "https": f"socks5://{proxies_host}:{proxies_port}",
    }

    code_res = f'''import random
import requests
import pyquery
import logging
import os
import wrapt
import traceback
import functools

logger_debug = logging.debug
logger_info = logging.info
logger_warn = logging.warning
logger_error = logging.error

USE_PROXY = True
# USE_PROXY = False
USE_CACHE = True
# USE_CACHE = False
PASS_EX = True
# PASS_EX = False
TMP_PATH_CACHE = 'tmp_cache_html'


def save_cache_to_file(if_read_cache_key=None):
    """加上装饰器就会保存函数结果到文件，装饰器设置if_read_cache_key字符床，函数定义时设置对应名称参数，并在调用时设置True可从文件读cache"""
    if if_read_cache_key and not os.path.exists(TMP_PATH_CACHE):
        os.makedirs(TMP_PATH_CACHE)

    @wrapt.decorator
    def wrapper(func, _instance, args, kwargs):
        cache_path = os.path.join(TMP_PATH_CACHE, func.__name__)
        if isinstance(if_read_cache_key, str) and kwargs.get(if_read_cache_key) and os.path.exists(cache_path):
            with open(cache_path, 'rb') as f:
                result = f.read()
            logger_info('[{{}}]函数读取到缓存: {{}} | {{}} | {{}}'.format(
                func.__name__, os.path.realpath(cache_path)[:-len(cache_path)], cache_path, repr(result)[:30]))
        else:
            result = func(*args, **kwargs)
            if result:
                with open(cache_path, 'wb') as f:
                    f.write(result)
                    logger_debug('[{{}}]函数结果缓存到: {{}} | {{}} |'.format(
                        func.__name__, os.path.realpath(cache_path)[:-len(cache_path)], cache_path))
        return result

    return wrapper


def pass_ex(if_ex_return=None, exs=None):
    """加上装饰器就会捕获所有异常，异常返回if_ex_return，exs中的异常会对应打印error，不再exs里面则打印堆栈信息"""
    if not exs:
        exs = {{
            requests.exceptions.ConnectionError: '[ConnectionError] 代理失效或URL失效',
            requests.exceptions.ConnectTimeout: '[ConnectTimeout] 连接握手超时',
            requests.exceptions.ReadTimeout: '[ReadTimeout] 读取数据过程中超时',
        }}

    @wrapt.decorator
    def wrapper(func, _instance, args, kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as ex:
            if type(ex) in exs:
                logger_warn('{{}} {{}} 将返回  {{}}'.format(exs[type(ex)], func.__name__, repr(if_ex_return)))
            else:
                logger_warn("[{{}}] {{}} {{}}\\n{{}}".format(type(ex), ex, func.__name__, traceback.format_exc()))
            return if_ex_return

    return wrapper


def get_proxies():
    if not USE_PROXY:
        return None
    proxies_host = '{proxies_host}'
    proxies_port = {proxies_port}
    proxies = {{
        "http": f"socks5://{{proxies_host}}:{{proxies_port}}",
        "https": f"socks5://{{proxies_host}}:{{proxies_port}}",
    }}
    return proxies


def get_ua():
    return random.choice([
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"])


@save_cache_to_file(if_read_cache_key='_use_cache')
def get_html_{url_format}(url=r"{url}", _use_cache=True):
    headers = {headers_str}
    proxies = get_proxies()
    res = requests.{data['method'].lower()}(
        url=url,
        headers=headers,
        verify=False,
        # stream=True,
        stream=False,
        proxies=proxies,
        timeout=125,
    )
    result = res.content
    logger_info('{{}}采集到{{}}  ->  {{}}'.format(f"使用代理[{{proxies.get('http')}}]" if proxies else "不使用代理", url, repr(result)[:50]))
    return result


get_html_{url_format} = pass_ex(if_ex_return='')(get_html_{url_format})


def task(url):
    pq_html = pyquery.PyQuery(get_html_{url_format}(url=url, _use_cache=USE_CACHE) or '<></>')('html')
    
    for item in pq_html:
        logger_debug(item.text)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    tmp_logger_debug = functools.partial(print, '[DEBUG]')
    logger_debug = lambda x: tmp_logger_debug(repr(x))
    task(r"{url}")
'''
    return code_res


def stdout_write(msg: str):
    sys.stdout.write(msg)
    sys.stdout.flush()


def stderr_write(msg: str):
    sys.stderr.write(msg)
    sys.stderr.flush()


# noinspection PyProtectedMember,PyUnusedLocal,PyIncorrectDocstring,DuplicatedCode
def nb_print(*args, sep=' ', end='\n', file=None, flush=True):
    print_raw = print
    args = (str(arg) for arg in args)  # REMIND 防止是数字不能被join
    if file == sys.stderr:
        stderr_write(sep.join(args))  # 如 threading 模块第926行，打印线程错误，希望保持原始的红色错误方式，不希望转成蓝色。
    elif file in [sys.stdout, None]:
        # 获取被调用函数在被调用时所处代码行数
        line = sys._getframe().f_back.f_lineno
        # 获取被调用函数所在模块文件名
        file_name = sys._getframe(1).f_code.co_filename
        # sys.stdout.write(f'"{__file__}:{sys._getframe().f_lineno}"    {x}\n')
        stdout_write(
            f'\033[0;34m{time.strftime("%H:%M:%S")}  "{file_name}:{line}"   \033[0;30;44m{sep.join(args)}\033[0m{end} \033[0m')  # 36  93 96 94
    else:
        print_raw(*args, sep=sep, end=end, file=file)


def gen_from_clipboard():
    text = clipboard.paste()
    nb_print(f"{'*' * 80}\n{text}\n{'*' * 80}")
    if text and text.strip().startswith('fetch("http'):
        return fetch_to_requests(text)


if __name__ == '__main__':
    res = gen_from_clipboard()
    # python -m d22d.utils.f2r
    print(res)
    with open('tmp_spider.py', 'w', encoding='utf-8') as f:
        f.write(res)
