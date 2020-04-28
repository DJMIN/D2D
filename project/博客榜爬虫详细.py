# /usr/bin/env python
# coding: utf-8

import requests
import os
import pyquery as pq
import time

from utils.db import XlsxIbyFileD
from utils.db import CsvD

img_path = './img_big'
ren_path = './ren_path'

if not os.path.exists(img_path):
    os.mkdir(img_path)

if not os.path.exists(ren_path):
    os.mkdir(ren_path)

headers = {
            'Origin': 'https://mobile.twitter.com',
            'content-type': 'application/json',
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) Ap'
                           'pleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3'
                           '770.80 Safari/537.36')
}


headers1 = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "Accept-Encoding": "gzip, deflate",
    # "Cookie": "__cfduid=d7ac31bfc0db76fe624ccfbdead5876c91567072715; _ga=GA1.2.1044011388.1567072717; __auc=a2733fcb16cdcd0098516aa6f33; __gads=ID=ab94668eb0467d64:T=1567072724:S=ALNI_MYEr0fq9d1rMqo5L7ExmV-_JFXH3A; __AF=d3e23948-ce63-4b44-8419-a114290f8e27; _gid=GA1.2.1017984232.1576650773",
    # "Cookie": "__cfduid=d7ac31bfc0db76fe624ccfbdead5876c91567072715; _ga=GA1.2.1044011388.1567072717; __auc=a2733fcb16cdcd0098516aa6f33; __gads=ID=ab94668eb0467d64:T=1567072724:S=ALNI_MYEr0fq9d1rMqo5L7ExmV-_JFXH3A; __AF=d3e23948-ce63-4b44-8419-a114290f8e27; _gid=GA1.2.1017984232.1576650773",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Host": 'cdn.thestandnews.com',
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
}


def main():
    xf = XlsxIbyFileD(path='./')
    try:
        all_data = [_ for _ in CsvD(path='./data').get_data('ren_db')]
    except FileNotFoundError:
        all_data = []
    for td in xf.get_data('123'):
        url = td['blob_url']
        print(url)
        if url in [_['url'] for _ in all_data]:
            continue
        data = []
        for j in range(40000):
            try:
                data = requests.get(url, headers=headers, proxies={
                    "http": "socks5://127.0.0.1:8388",
                    "https": "socks5://127.0.0.1:8388",
                }).text
                break
            except Exception as e:
                time.sleep(3)
                print(j, 'url', e)
        tmp = {
            'url': url,
            'name': '',
            'img_url': '',
            'm1': ''
        }

        for d in pq.PyQuery(data)('.author-profile.author-profile-with-photo').items():
            tmp = {
                'url': url,
                'name': d('h3.name').text().strip(),
                'img_url': d('.photo img').attr('src').strip(),
                'm1': d('p.profile').text().strip()
            }
            print(tmp)
        for d in pq.PyQuery(data)('.author-wrapper').items():
            tmp['name'] = tmp['name'] or d('h4.name').text().strip()
            tmp['img_url'] = tmp['img_url'] or d('img.author-photo').attr('src').strip()
            tmp['m1'] = tmp['m1'] or d('p.profile').text().strip()
            print(tmp)
        all_data.append(tmp)

        for j in range(3):
            try:
                with open(f'{img_path}/{tmp["name"]}.jpg', 'wb') as wf:
                    wf.write(requests.get(tmp['img_url'], headers=headers1, proxies={
                        "http": "socks5://127.0.0.1:8388",
                        "https": "socks5://127.0.0.1:8388",
                    }).content)
                    tmp['img_path'] = f'{img_path}/{tmp["name"]}.jpg'
                    break
            except Exception as e:
                time.sleep(3)
                tmp['img_path'] = ''
                print(j, e)
        if os.path.exists('./data/ren_db.csv'):
            os.remove('./data/ren_db.csv')
        cf = CsvD(path='./data')
        cf.create_index('ren_db', all_data[0])
        cf.save_data('ren_db', all_data)
        cf = None


if __name__ == '__main__':
    main()
