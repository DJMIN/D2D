# /usr/bin/env python
# coding: utf-8

import requests
import os
import pyquery as pq

from utils.db import XlsxIbyFileD

img_path = './img_small'

if not os.path.exists(img_path):
    os.mkdir(img_path)


def main():
    all_data = []
    xf = None
    for i in range(1, 22):
        url = 'https://www.thestandnews.com/authors/?page={}'.format(i)
        data = []
        for j in range(3):
            try:
                data = pq.PyQuery(requests.get(url, proxies={
                    "http": "socks5://127.0.0.1:2887",
                    "https": "socks5://127.0.0.1:2887",
                }).text)('.author.clearfix').items()
                break
            except Exception as e:
                print(j, 'url', e)
        for d in data:
            tmp = {
                'name': d('.author-name a').text().strip(),
                'img_url': d('.photo img').attr('src').strip(),
                'blob_url': d('.author-name a').attr('href').strip(),
                'm1': d('.article a').text().strip()
            }
            print(tmp)
            all_data.append(tmp)
            # for j in range(3):
            #     try:
            #         with open(f'{img_path}/{tmp["name"]}_small.jpg', 'wb') as wf:
            #             wf.write(requests.get(tmp['img_url'], proxies={
            #                 "http": "socks5://127.0.0.1:8388",
            #                 "https": "socks5://127.0.0.1:8388",
            #             }).content)
            #             break
            #     except Exception as e:
            #         print(j, e)
        xf = XlsxIbyFileD(path='./')
        xf.create_index('blob_info{}'.format(i), all_data[0])
        xf.save_data('blob_info{}'.format(i), all_data)


if __name__ == '__main__':
    main()