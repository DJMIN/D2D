from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
from task import Migration


data = MySqlD(host='192.168.0.216', port=3306, user='zzz', passwd='1234qwer', database='tggm').get_data('select value from task where tasktype=31')

t = Migration(
    database_from=MySqlD(host='192.168.0.216', port=3306, user='zzz', passwd='1234qwer', database='tggm'),

    database_to=MySqlD(host='192.168.0.216', port=3306, user='zzz', passwd='1234qwer', database='tggm'),
    table_from='select uuid, tag, priority, value, tasktype, peer_id, timelastproceed from task1 where tasktype!=1 limit 0,9999999',
    table_to='task',

    # database_to=CsvD(path='./dataa'),
    # table_from='select uuid, tag, priority, value from task where tasktype!=1',
    # table_to='url_tg',
    pks='uuid',
    quchong=True,
)

import re


def format_tg_group_link(link):
    link = link.strip()
    if link.find('t.me/s/') != -1:
        link = 't.me/' + link.split('t.me/s/', 1)[1]
    if link.find('t.me/') != -1:
        link = 't.me/' + link.rsplit('t.me/', 1)[1]
    link = link.replace('//', '/')
    link = link.split('?')[0]
    if link.endswith('/'):
        link = link[:-1]
    if re.fullmatch('t\.me/[\w\d_\-]+', link):
        link = link
    elif re.fullmatch('t\.me/[\w\d_\-^/]+/[\w\W%\d]+', link):
        if link[4:].find('/joinchat/') != -1:
            link = link
        else:
            link = link.rsplit('/', 1)[0]
    else:
        link = None
    return link


def formatd(s):
    if not s['value']:
        return
    s['value']=format_tg_group_link(s['value'])
    if s['value'][4:].find('/joinchat/') != -1:
        s['grouptype'] = 2
    else:
        s['grouptype'] = 1
    return s


t.format_data = formatd

t.run()
