import pymysql
import threading
import wrapt
import psycopg2
import psycopg2.extras
from functools import update_wrapper, wraps
from dbutils.persistent_db import PersistentDB
from dbutils.pooled_db import PooledDB
import time
import json
import logging
from functools import partial

from d22d.utils.decorators import timmer, flyweight, where_is_it_called, print_hz


log = logging.getLogger(__name__)


class BaseController(object):

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.last_res = None
        self._conf = {}
        self._conn = None
        self._cur = None
        self._persist = None

    @property
    def persist(self):
        if not self._persist:
            self._persist = PooledDB(**self._conf)
        return self._persist

    @staticmethod
    def get_conn(persist):
        return persist.connection()

    @property
    def conn(self):
        if not self._conn or getattr(self._conn, "_closed", True):
            self._conn = self.get_conn(self.persist)
        return self._conn

    @staticmethod
    def get_cur(conn):
        return conn.cursor()

    @property
    def cur(self):
        if not self._cur or getattr(self._cur, "_closed", True):
            self._cur = self.get_cur(self.conn)
        return self._cur

    def execute(self, sql, params=None, return_type=2, res_windows=10000, commit=False):
        """

        :param sql:  sql命令
        :param params:  sql参数
        :param res_windows:  返回时游标窗口
        :param return_type:  返回类型 1：dict 2：list（dict） 3：迭代器
        :param commit:  自动提交
        :return:
        """
        # conn = self.conn
        conn = self.get_conn(self.persist)
        cur = self.get_cur(conn)
        cur.execute(sql, params)
        if return_type == 1:
            resl = {}
        elif return_type == 2:
            resl = []
        elif return_type == 4:
            resl = None
        else:
            raise ValueError(f"return_type {return_type} not in [1,2,3,4]")
        while res := cur.fetchmany(res_windows):
            for d in res:
                if return_type == 1:
                    resl = d
                    break
                elif return_type == 2:
                    resl.append(d)
                elif return_type == 4:
                    resl = list(d.values())
                    if len(resl):
                        resl = resl[0]
                    break
        cur.close()  # or del cur
        if commit:
            conn.commit()
        conn.close()  # or del db
        return resl

    def execute_iter(self, sql, params=None, res_windows=10000, commit=False):
        """

        :param sql:  sql命令
        :param params:  sql参数
        :param res_windows:  返回时游标窗口
        :param return_type:  返回类型 1：dict 2：list（dict） 3：迭代器
        :param commit:  自动提交
        :return:
        """
        # conn = self.conn
        conn = self.get_conn(self.persist)
        cur = self.get_cur(conn)
        cur.execute(sql, params)
        while res := cur.fetchmany(res_windows):
            for d in res:
                yield d
        cur.close()  # or del cur
        if commit:
            conn.commit()
        conn.close()  # or del db

    @where_is_it_called
    def execute_auto(self, sql, params=None, return_type=2, commit=False, *args, **kwargs):
        """
        return返回值
        return_type=1  第一个字典
        return_type=2  字典列表
        return_type=3  字典迭代器
        return_type=4  第一列第一行的值
        """
        # with decorators.ExceptionContextManager() as _:
        if return_type == 3:
            res = self.execute_iter(sql=sql, params=params, commit=commit, *args, **kwargs)
        else:
            res = self.execute(sql=sql, params=params, return_type=return_type, commit=commit, *args, **kwargs)
        return res

    def sql_select(self, table, where, res_key='*', limit=10, return_type=2, *args, **kwargs):
        return self.execute_auto(
            f'select {res_key} from {table} where {where} limit {limit}',
            return_type=return_type, *args, **kwargs)

    def sql_select_eq(self, table, where, res_key='*', limit=10, return_type=2, *args, **kwargs):
        keys, vals = format_sql_where(where)
        if isinstance(res_key, list):
            res_key = ', '.join(res_key)
        return self.execute_auto(
            f'select {res_key} from {table} where {keys} limit {limit}', params=vals,
            return_type=return_type, *args, **kwargs)

    def update_data(self, table_name, data):
        if data.get('timeupdate', False) is not False:
            data['timeupdate'] = time.time()
        aa = gen_update_sql_safe(table_name, data)
        return self.execute_auto(*aa, return_type=2, commit=True)

    def mysql_update(self, table_name, where, kwargs):
        ks, vs = format_sql_kv(kwargs)
        return self.execute_auto(
            'update {} set {} where {}'.format(
                table_name,
                ks,
                where
            ), params=vs, return_type=2, commit=True)

    def update_some_windows(self, table_name, update, where, windows=20000):
        flag = self.execute_auto(
            f'select count(0) as cnt from {table_name} '
            f'where {where}', return_type=4)
        while flag:
            print(f'重置数据库tasks剩余： {flag}')
            self.execute_auto(
                f'update {table_name} set {update} '
                f'where {where} limit {windows}', commit=True)

            if flag > windows:
                flag -= windows
            else:
                flag = self.execute_auto(
                    f'select count(0) as cnt from {table_name} '
                    f'where {where}',
                    return_type=4)

    @timmer()
    def update_data_by_pk(self, _table_name_, _pk_, _data_: list, **kwargs):
        kwargs["timeupdate"] = time.time()
        ks, vs = format_sql_kv(kwargs)
        return self.execute_auto(
            'update {} set {} where {} in ({})'.format(
                _table_name_,
                ks,
                _pk_,
                ', '.join([str(s[_pk_]) for s in _data_])
            ), params=vs, return_type=2, commit=True)

    get_one = partial(sql_select, return_type=1)
    get_some = partial(sql_select, return_type=2)


# 享元模式（Flyweight Pattern）主要用于减少创建对象的数量，以减少内存占用和提高性能。这种类型的设计模式属于结构型模式，它提供了减少对象数量从而改善应用所需的对象结构的方式。
@flyweight
class MYSQLController(BaseController):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, port, user, password, database)
        self._conf = dict(
            # dbpai=pymysql,
            maxusage=1000,  # 一个连接最多复用次数，None为无限
            creator=pymysql,
            host=host,
            user=user,
            passwd=password,
            database=database if database else None,
            port=port,
            charset='utf8mb4',
            use_unicode=True,
            autocommit=False,
            # 流式、字典访问
            mincached=1,  # 初始化时，最小闲置数
            maxcached=10,  # 最大闲置数
            blocking=True,  # 无可用连接，等待
            # maxconnections=10, #创建连接池的最大数量
            # maxshared=20, #共享连接允许最大数量
            cursorclass=pymysql.cursors.SSDictCursor)


@flyweight
class PGController(BaseController):
    def __init__(self, host, port, user, password, database):
        super().__init__(host, port, user, password, database)
        self._conf = dict(
            maxusage=1000,
            creator=psycopg2,
            maxconnections=255,  # 连接池允许的最大连接数，0 和 None 表示不限制连接数
            mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0 表示不创建
            maxcached=32,  # 链接池中最多闲置的链接，0 和 None 不限制
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            setsession=[],  # 开始会话前执行的命令列表
            host=host,
            user=user,
            password=password,
            database=database if database else None,
            port=port,
        )

    @staticmethod
    def get_cur(conn):
        return conn.cursor(
                name=str(time.time()), cursor_factory=psycopg2.extras.RealDictCursor, scrollable=True, withhold=True)

    def show_databases(self):
        return list(d.get('datname') for d in self.execute_auto('select datname from pg_database;', return_type=3))

    def show_tables(self, database=None):
        return list(d.get('table_name') for d in self.execute_auto(
            # f"SELECT table_name FROM information_schema.tables WHERE table_schema='{database or self.database}';", return_type=3))
            f"SELECT table_name FROM information_schema.tables WHERE table_catalog='{database or self.database}';", return_type=3))

    def describe_table_name(self, table_name):
        return list(d.get('column_name') for d in self.execute_auto(
            f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}';", return_type=3))


def format_filed(filed):
    return ', '.join(f'{k}' for k in filed)


def format_sql_where(params):
    keys = []
    vals = []
    for k, v in params.items():
        keys.append(k)
        vals.append(v)
    return ' and '.join(f'{k}=%s' for k in keys), vals


def format_sql_kv(params):
    keys = []
    vals = []
    for k, v in params.items():
        keys.append(k)
        vals.append(v)
    return ', '.join(f'{k}=%s' for k in keys), vals


def gen_update_sql_safe(table_name, kvs):
    if len(kvs) == 0:
        return ''

    keys = []
    values = []
    for k, v in kvs.items():
        keys.append("`{}`".format(k))
        if isinstance(v, (tuple, set)):
            v = list(v)
        if isinstance(v, (dict, list)):
            v = json.dumps(v)
        values.append(v)
    sql = f'insert into {table_name}({",".join(keys)}) ' \
          f'values({",".join(["%s"] * len(values))})' \
          f'ON DUPLICATE KEY UPDATE {",".join("`%s`=%%s" % k for k in kvs.keys())}'

    return sql, values * 2
