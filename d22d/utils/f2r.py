import re
import json
import sys
import time
import urllib.parse
import clipboard


def fetch_to_requests(fetch):
    raw = fetch.strip().split('fetch(', 1)[-1][:-2]
    url, data = re.fullmatch(r'"(.*)", {(.*)', raw, re.DOTALL).groups()
    url_d = urllib.parse.urlsplit(url)
    # print(url_d.netloc)
    # print(url_d.fragment)
    # print(url_d.scheme)
    # print(url_d.hostname)
    data = json.loads(f'{{{data}')

    headers = data['headers']
    headers["Host"] = url_d.netloc
    headers["Referer"] = data['referrer'] or url_d
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
    headers_str = f'{{"User-Agent": get_ua(), {json.dumps(headers)[1:-1]}}}'
    proxies_host = '127.0.0.1'
    proxies_port = 8389
    proxies = {
        "http": f"socks5://{proxies_host}:{proxies_port}",
        "https": f"socks5://{proxies_host}:{proxies_port}",
    }
    code_res = f"""import random
import requests


def get_proxies():
    return None
    return {proxies}


def get_ua():
    return  random.choice([
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"])


def get_html(data=None):
    headers = {headers_str}
    res = requests.{data['method'].lower()}(
        url=r"{url}",
        headers=headers,
        verify=False,
        # stream=True,
        stream=False,
        proxies=get_proxies(),
        timeout=125,
    )
    return res.text


if __name__ == '__main__':
    print(get_html())
"""
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
    print(res)
    with open('tmp_spider.py', 'w', encoding='utf-8') as f:
        f.write(res)
