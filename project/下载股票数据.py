from utils import MySqlD
from utils import CsvD
import baostock as bs
import pandas as pd
import datetime

#### 登陆系统 ####
lg = bs.login(user_id="anonymous", password="123456")
#### 获取沪深A股估值指标(日频)数据 ####
fanyi = {
    'date': '交易所行情日期',  # 格式：YYYY-MM-DD
    'code': '证券代码',  # 格式：sh.600000。sh：上海，sz：深圳
    'open': '今开盘价格',  # 精度：小数点后4位；单位：人民币元
    'high': '最高价',  # 精度：小数点后4位；单位：人民币元
    'low': '最低价',  # 精度：小数点后4位；单位：人民币元
    'close': '今收盘价',  # 精度：小数点后4位；单位：人民币元
    'preclose': '昨日收盘价',  # 精度：小数点后4位；单位：人民币元
    'volume': '成交数量',  # 单位：股
    'amount': '成交金额',  # 精度：小数点后4位；单位：人民币元
    'adjustflag': '复权状态',  # 不复权、前复权、后复权
    'turn': '换手率',  # 精度：小数点后6位；单位：%
    'tradestatus': '交易状态',  # 1：正常交易 0：停牌
    'pctChg': '涨跌',  # （百分比）	精度：小数点后6位
    'peTTM': '滚动市盈率',  # 精度：小数点后6位
    'psTTM': '滚动市销率',  # 精度：小数点后6位
    'pcfNcfTTM': '滚动市现率',  # 精度：小数点后6位
    'pbMRQ': '市净率',  # 精度：小数点后6位
    'isST': '是否ST',  # 1是，0否
}


def get_k(piao, date):
    print(date,
          (datetime.datetime.fromisoformat(date) + datetime.timedelta(days=31 * 12 * 21)).isoformat().split('T')[0])
    rs = bs.query_history_k_data_plus(piao,
                                      ','.join(fanyi.keys()),
                                      start_date=date, end_date=(
                    datetime.datetime.fromisoformat(date) + datetime.timedelta(days=31 * 12 * 21)).isoformat().split(
            'T')[0],
                                      frequency="d", adjustflag="3")
    return rs


def get_all_k():
    #### 打印结果集 ####

    for idx,( piao, name, industry) in enumerate(
            ['sh.000300', '上证综指', '大盘']+
            [( p[1], p[2], p[3] ) for p in pd.read_csv(open(
            './stock_industry.csv', 'r', encoding="gbk")).values.tolist()]
    ):
        print(idx)
        result_list = []
        for date in [
            '2000-01-01'
        ]:
            # piao='sh.000300'
            # piao = 'sz.300032'
            # rs = get_k('sh.881122', date)
            rs = get_k(piao, date)
            # rs = get_k('sz.300032', date)
            while (rs.error_code == '0') & rs.next():
                # 获取一条记录，将记录合并在一起
                result_list.append(rs.get_row_data())
        result = pd.DataFrame(result_list, columns=rs.fields)
        result.rename(columns=fanyi, inplace=True)
        #### 结果集输出到csv文件 ####
        result.to_csv(f"./data/gu_{industry}_{piao}_{name}_data.csv".replace("*",""), encoding="gbk", index=False)
        print(piao)

get_all_k()
#### 登出系统 ####
bs.logout()

# MySqlD(host='localhost', port=3306, database='gu',
#        user='root', passwd='root'),
