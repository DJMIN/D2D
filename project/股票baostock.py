import baostock as bs
import pandas as pd
import datetime

#### 登陆系统 ####
lg = bs.login(user_id="anonymous", password="123456")
#### 获取沪深A股估值指标(日频)数据 ####
fanyi = {'peTTM': '动态市盈率',
'psTTM':'市销率',
'pcfNcfTTM':'市现率',
'pbMRQ':'市净率',}

def get_k(piao, date):
    print(date,(datetime.datetime.fromisoformat(date)+datetime.timedelta(days=31*12*2)).isoformat().split('T')[0] )
    rs = bs.query_history_k_data_plus(piao,
                                 "date,code,close,peTTM,pbMRQ,psTTM,pcfNcfTTM",
                                 start_date=date, end_date=(datetime.datetime.fromisoformat(date)+datetime.timedelta(days=31*12*7)).isoformat().split('T')[0],
                                 frequency="d", adjustflag="3")
    print(rs)
    return rs


def get_all_k():
    #### 打印结果集 ####
    result_list = []
    for date in [
        '2017-05-01',
        # '2017-11-13',
        # '2017-11-14',
        # '2017-11-15',
        # '2017-11-16',
        # '2017-11-17',
        # '2017-11-20',
        # '2017-11-21',
        # '2017-11-22',
        # '2017-11-23',
        # '2017-11-24',
        # '2018-02-27',
        # '2018-05-15',
        # '2018-05-16',
        # '2018-05-17',
        # '2018-05-18',
        # '2018-05-21',
        # '2018-05-22',
        # '2018-05-23',
        # '2018-05-24',
        # '2018-05-25',
        # '2018-05-28',
        # '2018-05-29',
    ]:
        # piao='sh.000300'
        piao='sz.300032'
        # rs = get_k('sh.881122', date)
        rs = get_k(piao, date)
        # rs = get_k('sz.300032', date)
        while (rs.error_code == '0') & rs.next():
            # 获取一条记录，将记录合并在一起
            result_list.append(rs.get_row_data())
    result = pd.DataFrame(result_list, columns=rs.fields)
    result.rename(columns=fanyi, inplace=True)
    #### 结果集输出到csv文件 ####
    result.to_csv(f"peTTM_{piao}_data.csv", encoding="gbk", index=False)
    print(result)

get_all_k()
#### 登出系统 ####
bs.logout()