#!/usr/bin/env python
# coding: utf-8

import sys
import re
import logging
import traceback
import os
import warnings

import openpyxl
import openpyxl.utils
import time
import datetime
import json
import hashlib
import elasticsearch
import elasticsearch.helpers
from six import itervalues
# from werkzeug.utils import text_type
import psycopg2 as d_b_c
import psycopg2

INSERT_NORMAL = 0
INSERT_REPLACE = 1
INSERT_IGNORE = 2
import mysql.connector as d_b_c
import mysql.connector as mysql_connector
import pymysql
from mysql.connector.errors import DatabaseError

cur_e=(d_b_c.OperationalError, d_b_c.InterfaceError)
INSERT_MODES = {INSERT_IGNORE: 'insert ignore', INSERT_REPLACE: 'replace', INSERT_NORMAL: 'insert'}
ESCAPE="`%s`"
PY2 = sys.version_info[0] == 2
WIN = sys.platform.startswith('win')

# ESCAPE = '"%s"'
# cur_e = Exception
# INSERT_MODES = {INSERT_IGNORE: 'insert', INSERT_REPLACE: 'replace', INSERT_NORMAL: 'insert'}
"""
PostgreSQL 中简单实现 Insert ignore 操作
create 
    rule [规则名称] as on insert to [表名] 
where 
    exists 
        (select 1 from [表名] where [判断条件]) 
    do instead nothing; """

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    fmt='[%(levelname)s %(asctime)s.%(msecs)03d] [%(process)d:%(threadName)s:%(funcName)s:%(lineno)d] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
LOG.addHandler(handler)


def call_cost(func):
    def handler(*args, **kwargs):
        t = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            t = round(time.time() - t, 3)
            logging.info(u'函数 {} 耗时 {} 秒'.format(func.__name__, t))

    return handler


class ExcelWriter:
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

    def __init__(self, filename=None):
        self.wb = None
        self.ws = None
        self.f_idx = 0
        self.filename = filename
        time_now = time.time()
        ms = int((time_now - int(time_now)) * 1000000)
        self.filename_auto = '{}-{}-{:06d}'.format(
            os.path.basename(sys.argv[0]).split(".")[0], datetime.datetime.now().strftime('%Y%m%d%H%M%S'), ms)
        self.new_file(self.filename)

    def write_row(self, row, indices=None):
        try:
            self.ws.append(row)
        except openpyxl.utils.exceptions.IllegalCharacterError as ex:
            if not indices:
                logging.warning('Failed to write excel: {}'.format(ex))
                return
            for index in indices:
                row[index] = re.sub(self.ILLEGAL_CHARACTERS_RE, '', row[index])
            try:
                self.ws.append(row)
            except Exception as ex:
                logging.warning('Failed to write excel: {}\n{}'.format(ex, traceback.format_exc()))

    @call_cost
    def new_file(self, filename=None):
        self.wb = openpyxl.Workbook()
        self.ws = self.wb.create_sheet(index=0)
        self.filename = filename or 'res-{}-{}.xlsx'.format(self.filename_auto, self.f_idx)

    @call_cost
    def save(self):
        self.wb.save(self.filename)
        logging.info('write excel fin: {}'.format(self.filename))

    @call_cost
    def write_all_excel(self, result):
        res = []
        for _ in result:
            res.append(_)
        result = res
        keys = [key for key in result[0].keys()]
        self.ws.append(keys)
        per_file_num = 0
        for idx, d in enumerate(result):
            if not idx % 50000:
                logging.info('write excel: {}/{} ..{:.2f}%'.format(idx, len(result), idx / (len(result) or 1) * 100))
            row = [str(d.get(key, '')) for key in keys]
            self.write_row(row)
            per_file_num += 1
            if per_file_num > 900000:
                self.save()
                self.f_idx += 1
                per_file_num = 0
                self.new_file()
                self.ws.append(keys)
        self.save()


class T:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def secure_filename(filename):
    if isinstance(filename, str):
        from unicodedata import normalize
        filename = normalize('NFKD', filename).encode('utf-8', 'ignore')  # 转码
        if not PY2:
            filename = filename.decode('utf-8')  # 解码
    for sep in os.path.sep, os.path.altsep:
        if sep:
            filename = filename.replace(sep, ' ')

    # 正则增加对汉字和日语假名的部分（本人有需求）
    # \ / : * ? " < > |    r"[\/\\\:\*\?\"\<\>\|]"
    # [ \\[ \\] \\^ \\-_*×――(^)$%~!@#$…&%￥—+=<>《》!！??？:：•`·、。，；,.;\"‘’“”-]
    # \u4E00-\u9FBF 中文
    # \u3040-\u30FF 假名
    # \u31F0-\u31FF 片假名扩展
    # _filename_ascii_add_strip_re = re.compile(r'[^A-Za-z0-9_\u4E00-\u9FBF\u3040-\u30FF\u31F0-\u31FF.-]')
    _filename_ascii_add_strip_re = re.compile(r"[\/\\\:\*\?\"\<\>\|]")

    filename = str(_filename_ascii_add_strip_re.sub('', '_'.join(  # 新的正则
        filename.split()))).strip('._')

    _windows_device_files = ('CON', 'AUX', 'COM1', 'COM2', 'COM3', 'COM4', 'LPT1',
                             'LPT2', 'LPT3', 'PRN', 'NUL')

    # on nt a couple of special files are present in each folder.  We
    # have to ensure that the target file is not such a filename.  In
    # this case we prepend an underline
    if os.name == 'nt' and filename and \
       filename.split('.')[0].upper() in _windows_device_files:
        filename = '_' + filename

    return filename[:250]


def mkdir(file_name):
    path = os.path.join(get_realpath(), file_name)
    if not os.path.exists(path):
        os.mkdir(path)
    return path


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


class EsModel(object):
    def __init__(self, hosts, timeout=120):
        self.hosts = hosts
        self._es_client = elasticsearch.Elasticsearch(hosts=self.hosts, timeout=timeout)

    def get_info(self):
        return self.es.info()

    def analyze(self, query, analyzer='standard'):
        iclient = elasticsearch.client.indices.IndicesClient(self.es)
        return iclient.analyze(body={'text': query, 'analyzer': analyzer})

    def scan(
            self,
            query=None,
            scroll="30m",
            raise_on_error=True,
            preserve_order=False,
            size=1000,
            request_timeout=None,
            clear_scroll=True,
            scroll_kwargs=None,
            **kwargs
    ):
        """
        Simple abstraction on top of the
        :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
        yields all hits as returned by underlining scroll requests.

        By default scan does not return results in any pre-determined order. To
        have a standard order in the returned documents (either by score or
        explicit sort definition) when scrolling, use ``preserve_order=True``. This
        may be an expensive operation and will negate the performance benefits of
        using ``scan``.

        :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
        :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :arg raise_on_error: raises an exception (``ScanError``) if an error is
            encountered (some shards fail to execute). By default we raise.
        :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
            cause the scroll to paginate with preserving the order. Note that this
            can be an extremely expensive operation and can easily lead to
            unpredictable results, use with caution.
        :arg size: size (per shard) of the batch send at each iteration.
        :arg request_timeout: explicit timeout for each call to ``scan``
        :arg clear_scroll: explicitly calls delete on the scroll id via the clear
            scroll API at the end of the method on completion or error, defaults
            to true.
        :arg scroll_kwargs: additional kwargs to be passed to
            :meth:`~elasticsearch.Elasticsearch.scroll`

        Any additional keyword arguments will be passed to the initial
        :meth:`~elasticsearch.Elasticsearch.search` call::

            scan(es,
                query={"query": {"match": {"title": "python"}}},
                index="orders-*",
                doc_type="books"
            )

        """
        scroll_kwargs = scroll_kwargs or {}

        if not preserve_order:
            query = query.copy() if query else {}
            query["sort"] = "_doc"

        # initial search
        resp = self.es.search(
            body=query, scroll=scroll, size=size, request_timeout=request_timeout, **kwargs
        )
        scroll_id = resp.get("_scroll_id")

        try:
            while scroll_id and resp["hits"]["hits"]:
                for hit in resp["hits"]["hits"]:
                    yield hit

                # check if we have any errors

                # if (resp["_shards"]["successful"] + resp["_shards"]["skipped"]) < resp["_shards"]["total"]:
                #     logging.warning(
                #         "Scroll request has only succeeded on %d (+%d skipped) shards out of %d.",
                #         resp["_shards"]["successful"],
                #         resp["_shards"]["skipped"],
                #         resp["_shards"]["total"],
                #     )
                #     if raise_on_error:
                #         raise Exception(
                #             scroll_id,
                #             "Scroll request has only succeeded on %d (+%d skiped) shards out of %d."
                #             % (resp["_shards"]["successful"], resp["_shards"]["skipped"], resp["_shards"]["total"]),
                #         )

                resp = self.es.scroll(
                    body={"scroll_id": scroll_id, "scroll": scroll}, **scroll_kwargs
                )
                scroll_id = resp.get("_scroll_id")

        finally:
            if scroll_id and clear_scroll:
                self.es.clear_scroll(body={"scroll_id": [scroll_id]}, ignore=(404,))

    # def scan(self, query, index, doc_type='_doc', scroll_id=None, scroll_total=0, scroll='30m',
    #          preserve_order=False, window=1000, size=None, offset=0, fields=None,
    #          clear_scroll=True, sort=None, sleep=0, query_task_id=None,
    #          **scroll_kwargs):
    #     query_str = str(json.dumps(query).replace("'", '"'))
    #     print(query_str)
    #     if not scroll_id:
    #         if not preserve_order:
    #             query = query.copy() if query else {}
    #             query["sort"] = "_doc"
    #
    #         # initial search
    #         resp = self.es.search(body=query, index=index, scroll=scroll, size=size, sort=sort)
    #
    #         scroll_id = resp.get('_scroll_id')
    #         scroll_total = resp.get('hits', {}).get('total')['value']
    #         first_run = True
    #
    #     else:
    #         first_run = False
    #         scroll_total = scroll_total
    #         resp = {}
    #     fin = 0
    #     if scroll_id is None:
    #         return
    #     try:
    #         idx  = 0
    #         while True:
    #             idx+=1
    #             # if we didn't set search_type to scan initial search contains data
    #             if first_run:
    #                 first_run = False
    #             else:
    #                 print(idx,scroll_id)
    #
    #                 resp = self.es.scroll(scroll_id=scroll_id)
    #                                       # params={'source': [] if fin < offset else fields},
    #                                       # source=[] if fin < offset else fields,)
    #             for hit in resp['hits']['hits']:
    #                 yield hit
    #             if size:
    #                 scroll_id = resp.get('_scroll_id')
    #                 print(resp, scroll_id)
    #             # check if we have any errrors
    #             # if resp["_shards"]["successful"] < resp["_shards"]["total"]:
    #             #     raise Exception(
    #             #         'Scroll request has only succeeded on %d shards out of %d.' % (
    #             #         resp['_shards']['successful'], resp['_shards']['total'])
    #             #     )
    #
    #             # end of scroll
    #             if scroll_id is None or not resp['hits']['hits'] or (
    #                     size and fin - offset >= size) or scroll_total == fin:
    #                 break
    #     finally:
    #         if scroll_id and clear_scroll:
    #             self.es.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404,))

    def select(self, query, index, doc_type="_doc", fields=None, offset=0, limit=0, sort=None, result_format=True):
        offset = offset or 0
        limit = limit or 0
        if not limit:
            for record in elasticsearch.helpers.scan(self.es, index=index, doc_type=doc_type, query=query,
                                                     _source_include=fields, from_=offset, sort=sort):
                yield record['_source'] if result_format else record
        else:
            for record in self.es.search(index=index, doc_type=doc_type,
                                         body=query, _source_include=fields, from_=offset, size=limit,
                                         sort=sort
                                         ).get('hits', {}).get('hits', []):
                yield record['_source'] if result_format else record

    def search(self, query, index, doc_type="_doc", fields=None, offset=0, limit=0, sort=None):
        return self.es.search(index=index, doc_type=doc_type, body=query,
                              _source_include=fields, from_=offset, size=limit,
                              sort=sort)

    def msearch(self, query, index, doc_type="_doc"):
        body = '{}\n%s' % '\n{}\n'.join([json.dumps(q) for q in query])  # header\n body\n
        return self.es.msearch(index=index, doc_type=doc_type, body=body)['responses']

    def msearch_format(self, query, index, doc_type="_doc"):
        body = '{}\n%s' % '\n{}\n'.join([json.dumps(q) for q in query])  # header\n body\n
        return [r['hits']['hits'][0] for r in self.es.msearch(
            index=index, doc_type=doc_type, body=body)['responses'] if r['hits']['hits']]

    def count(self, index, doc_type="_doc", query=None):
        temp_query = {"query": query.get('query', {})}
        if not temp_query['query']:
            temp_query.pop('query')
        return self.es.count(index=index, doc_type=doc_type,
                             body=temp_query).get('count', 0)

    def get(self, es_id, index, doc_type="_doc", fields=None):
        ret = self.es.get(index=index, doc_type=doc_type, id=es_id,
                          _source_include=fields, ignore=404)
        return ret.get('_source', None)

    def drop(self, query, index, doc_type="_doc"):
        self.refresh(index)
        for record in elasticsearch.helpers.scan(self.es, index=index, doc_type=doc_type, query=query, _source=False):
            self.es.delete(index=index, doc_type=doc_type, id=record['_id'])

    def refresh(self, index):
        """
        Explicitly refresh one or more index, making all operations
        performed since the last refresh available for search.
        """
        self.es.indices.refresh(index=index)

    def copy(self):
        """
        Explicitly refresh one or more index, making all operations
        performed since the last refresh available for search.
        """
        return self

    @property
    def es(self):
        return self._es_client

    def bulk_write(self, data, batch_size=1000, retry=3):
        total = len(data)
        if retry < 0:
            retry = -retry
        elif retry > 10:
            retry = 10
        succeed = 0

        for _ in range(retry):
            try:
                result = elasticsearch.helpers.bulk(self.es, data, request_timeout=150, chunk_size=batch_size,
                                                    raise_on_error=False, raise_on_exception=False)
                succeed = result[0]
                if succeed == total:
                    return succeed
            except Exception as e:
                logging.error(e, traceback.format_exc())
        return succeed

    @staticmethod
    def _add_time_range(query, field='created_at', starttime=None, endtime=None):
        if starttime or endtime:
            time_range = {'range': {field: {'format': 'epoch_second'}}}
            if starttime:
                time_range['range'][field]['gte'] = starttime
            if endtime:
                time_range['range'][field]['lte'] = endtime
            query['query']['bool']['filter'].append(time_range)
        return query

    @staticmethod
    def _add_highlight(query):
        query['highlight'] = {
            "number_of_fragments": 0,  # fragment 是指一段连续的文字。返回结果最多可以包含几段不连续的文字。默认是5。
            "fragment_size": 0,  # 一段 fragment 包含多少个字符。默认100。
            "require_field_match": False,
            "pre_tags": "<font color=\"red\">",
            "post_tags": "</font>",
            "encoder": "html",
            "fields": {
                "*": {}
            }
        }
        return query

    @staticmethod
    def _add_aggregations_screen_name(query, offset=0, limit=2147483647):
        """
        "aggregations" : {
            "<aggregation_name>" : { <!--聚合的名字 -->
                "<aggregation_type>" : { <!--聚合的类型 -->
                    <aggregation_body> <!--聚合体：对哪些字段进行聚合 -->
                }
                [,"meta" : {  [<meta_data_body>] } ]? <!--元 -->
                [,"aggregations" : { [<sub_aggregation>]+ } ]? <!--在聚合里面在定义子聚合 -->
            }
            [,"<aggregation_name_2>" : { ... } ]*<!--聚合的名字 -->
        }

        聚合后分页详情见 https://blog.csdn.net/laoyang360/article/details/79112946

        :param query:
        :return:
        """
        query['size'] = 0
        query['from'] = offset
        query['aggs'] = {
            "group_agg": {
                "terms": {
                    "field": "user_screen_name",
                    "size": limit,
                    # 'from': offset,
                    "order": [{"_count": "desc"}],
                    # 广度搜索方式数据量越大，那么默认的使用深度优先的聚合模式生成的总分组数就会非常多，
                    # 但是预估二级的聚合字段分组后的数据量相比总的分组数会小很多所以这种情况下使用广度优先的模式能大大节省内存
                    "collect_mode": "breadth_first",
                },

                "aggs": {
                    "%s_%s" % ('user_screen_name', 'top_hits'): {
                        "top_hits": {
                            "size": 1,
                            "sort": [
                                {
                                    "created_at": {
                                        "order": "desc"
                                    }
                                }
                            ],
                            "_source": {
                                "includes": ['user_screen_name', 'text', 'id', 'retweeted']}}}}}}
        return query

    @staticmethod
    def _replace_highlight(data, keyword='text'):
        if keyword not in data['highlight']:
            return data
        if isinstance(data['highlight'][keyword], list):
            data['_source'][keyword] = ''.join(data['highlight'][keyword])
        else:
            data['_source'][keyword] = data['highlight'][keyword]
        return data


class ClientPyMySQL:
    INSERT_NORMAL = 'insert'
    INSERT_REPLACE = 'replace'
    INSERT_IGNORE = 'insert ignore'
    INSERT_MODES = {INSERT_IGNORE, INSERT_REPLACE, INSERT_NORMAL}
    PLACEHOLDER = '%s'

    def __init__(self, host, port, database,
                 user, passwd):
        # Connect to mysql.
        # dbc = pymysql.connect(
        #     host=Config.MYSQL_HOST, user=Config.MYSQL_USER,
        #     password=Config.MYSQL_PASSWORD, database=Config.MYSQL_DATABASE,
        #     port=Config.MYSQL_PORT,
        #     charset='utf8mb4',
        #     use_unicode=True,
        #     cursorclass=pymysql.cursors.DictCursor
        # )
        """
        SteadyDB
        DBUtils.SteadyDB基于兼容DB-API 2接口的数据库模块创建的普通连接，
        实现了"加强"连接。具体指当数据库连接关闭、丢失或使用频率超出限制时，将自动重新获取连接。

        典型的应用场景如下：在某个维持了某些数据库连接的程序运行时重启了数据库，
        或在某个防火墙隔离的网络中访问远程数据库时重启了防火墙。
        """

        import pymysql
        from dbutils.steady_db import connect
        self.dbc = connect(
            creator=pymysql,
            host=host, user=user,
            passwd=passwd,
            database=database if database else None,
            port=port,
            charset='utf8mb4',
            use_unicode=True,
            autocommit=False,
            # 流式、字典访问
            cursorclass=pymysql.cursors.SSDictCursor)

    @property
    def dbcur(self):
        try:
            # if self.dbc.unread_result:
            #     self.dbc.get_rows()
            return self.dbc.cursor()
        except (mysql_connector.OperationalError, mysql_connector.InterfaceError):
            self.dbc.ping(reconnect=True)
            return self.dbc.cursor()

    # def begin_transaction(self):
    #     self.dbc.begin()
    #
    # def end_transaction(self):
    #     self.dbc.commit()

    def begin_transaction(self):
        return self.dbcur.execute("BE"+"GIN;")

    def end_transaction(self):
        return self.dbcur.execute("COMMIT;")

    def rollback(self):
        return self.dbcur.execute("rollback;")

    def fetch(self, dbcur):
        result = dbcur.fetchone()
        while result is not None:
            result = dbcur.fetchone()

    # def _execute1(self, sql, values=None):
    #     print(sql)
    #     with self.dbcur as dbcur:
    #         try:
    #             if values:
    #                 dbcur.execute(sql, values or [])
    #             else:
    #                 dbcur.execute(sql+';')
    #
    #             def _fetch(_dbcur):
    #                 result = _dbcur.fetchmany(2)
    #                 print(result)
    #                 while result is not None:
    #                     for r in result:
    #                         yield r
    #                     result = _dbcur.fetchmany(2)
    #             return dbcur.lastrowid, _fetch(dbcur)
    #         except mysql_connector.DatabaseError as ex:
    #             if ex.errno == 1205:
    #                 logging.critical(traceback.format_exc())
    #             else:
    #                 raise

    def _execute(self, sql, values=None):
        print(sql)
        dbcur = self.dbc.cursor()
        try:
            if values:
                dbcur.execute(sql, values or [])
            else:
                dbcur.execute(sql)

            def _fetch(_dbcur):
                result = _dbcur.fetchmany(1000)
                while result:
                    # print(result)
                    for r in result:
                        yield r
                    result = _dbcur.fetchmany(1000)
            return dbcur.lastrowid, _fetch(dbcur)
        except mysql_connector.DatabaseError as ex:
            if ex.errno == 1205:
                logging.critical(traceback.format_exc())
            else:
                raise

    @staticmethod
    def escape(string):
        return '`%s`' % string

    def save_into_db(self, sql, datas, batch_size=1000):
        for i in range((len(datas) + batch_size - 1) // batch_size):
            self.begin_transaction()
            self._executemany(sql, datas[i * batch_size:(i + 1) * batch_size])
            self.end_transaction()

    def _executemany(self, sql_query, params):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dbcur = self.dbcur
            # logging.debug("<sql: %s>" % sql_query)
            dbcur.execute("BE" + "GIN;")
            dbcur.executemany(sql_query, params)
            dbcur.execute("COMMIT;")
            return dbcur.fetchall()

    def _get_insert_sql(self, _mode=INSERT_NORMAL, _tablename=None, **values):
        if _mode not in self.INSERT_MODES:
            return None

        tablename = self.escape(_tablename)
        res_values = []
        res_keys = []
        if values:
            for k, v in values.items():
                res_keys.append(k)
                res_values.append(v)
            _keys = ", ".join((self.escape(k) for k in res_keys))
            _values = ", ".join([self.PLACEHOLDER, ] * len(values))
            sql_query = "%s INTO %s (%s) VALUES (%s)" % (_mode, tablename, _keys, _values)
        else:
            sql_query = "%s INTO %s DEFAULT VALUES" % (_mode, tablename)
        logging.debug("<sql: %s>", sql_query)
        return sql_query, res_keys, res_values

    def _insert(self, _mode=INSERT_NORMAL, _tablename=None, **values):
        sql, _, _ = self._get_insert_sql(_mode, _tablename, **values)

        if values:
            dbcur = self._execute(sql, list(itervalues(values)))
        else:
            dbcur = self._execute(sql)
        return dbcur.lastrowid

    def insert_many_with_dict_list(
            self, tablename, data,
            mode='INSERT IGNORE',
            # mode='REPLACE',
            batch_size=5000):
        if not data:

            return

        tablename = self.escape(tablename)
        values = data[0]
        if values:
            _keys = ", ".join((self.escape(k) for k in values))
            _values = ", ".join([self.PLACEHOLDER, ] * len(values))
            # sql_query = "%s INTO %s (%s) VALUES (%s)" % (mode, tablename, _keys, _values)
            sql_query = "%s INTO %s (%s) VALUES (%s)" % (mode, tablename, _keys, _values)
        else:
            # sql_query = "%s INTO %s DEFAULT VALUES" % (mode, tablename)
            sql_query = "%s INTO %s DEFAULT VALUES" % (mode, tablename)
        # self.logger.debug("<sql: %s>", sql_query)
        # self.logger.debug("%s", tuple(data[0][k] for k in values))

        size = max([1, (len(data) + batch_size - 1) // batch_size])
        for i in range(size):
            # self.logger.info("{} {}/{}".format(mode, batch_size * (i + 1), len(data)))
            self._executemany(sql_query, [list(d[k] for k in values) for d in data[i * batch_size:(i + 1) * batch_size]])
            # self.begin_transaction()
            # self._executemany(sql_query, [list(d[k] for k in values) for d in data[i * batch_size:(i + 1) * batch_size]])
            # self.end_transaction()
        return len(data)


class BaseDB(object):
    placeholder = '%s'

    def __init__(self, host, port, database,
                 user, passwd, logger=None, db_init=False):
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or LOG
        self.db_init = db_init
        self.conn = None
        self.init_db()

    def init_db(self):
        self.conn = d_b_c.connect(user=self.user, password=self.passwd,
                                  host=self.host, port=self.port,
                                  database=self.database,
                                  charset='utf8',
                                  # use_unicode=True,
                                  # autocommit=False,
                                  # 流式、字典访问
                                  # , autocommit=True
                                  )
        database = self.database
        try:
            print([x[0] for x in self._execute('show databases')])
            if database not in [x[0] for x in self._execute('show databases')]:
                self.conn.cursor().execute('CREATE DATABASE if not exists %s default charset utf8' % database)
            self.conn.database = self.database
        except cur_e:
            pass

        if self.db_init:
            self.create_db()

    def set_autocommit(self, autocommit):
        try:
            self.conn.set_autocommit(autocommit)
        except:
            self.conn.autocommit = autocommit

    def create_db(self):
        raise NotImplementedError

    def close(self):
        self.conn.commit()
        self.conn.close()

    @staticmethod
    def escape(string):
        # return '`%s`' % string
        return ESCAPE % string

    @property
    def dbcur(self):
        try:
            if self.conn.unread_result:
                self.conn.get_rows()
            return self.conn.cursor()
        except cur_e:
            try:
                self.conn.rollback()
            except:
                pass
            try:
                self.conn.ping(reconnect=True)
                self.conn.database = self.database
            except:
                pass
            return self.conn.cursor()

    def begin_transaction(self):
        self.dbcur.execute("BEGIN;")

    def end_transaction(self):
        self.dbcur.execute("COMMIT;")

    def rollback(self):
        self.dbcur.execute("rollback;")

    def _execute(self, sql_query, values=None, multi=False):
        if values is None:
            values = []
        dbcur = self.dbcur
        self.logger.debug("<sql: %s>" % sql_query)
        try:
            dbcur.execute(sql_query, values
                          # , multi=multi
                          )
            return dbcur
        except DatabaseError as ex:
            errno = ex.errno
            if errno == 1205:
                # 超过了锁定等待超时
                logging.critical(traceback.format_exc())
            elif errno in (1062, 1146):
                # 键重复条目(1062) 表不存在(1146)
                logging.warning(ex)
            else:
                raise

    def _executemany(self, sql_query, params, raise_on_error=True):
        dbcur = self.dbcur
        # self.logger.debug("<sql: %s>" % sql_query)
        try:
            dbcur.executemany(sql_query, params)
            return dbcur
        except DatabaseError as ex:
            if not raise_on_error and (ex.errno == 1205 or ex.errno == 1062):
                logging.critical(traceback.format_exc())
            else:
                raise

    def _select(self, tablename=None, what="*", where="", where_values=None,
                group_by=None, order_by=None, offset=0, limit=None):
        if where_values is None:
            where_values = []
        tablename = self.escape(tablename)
        if isinstance(what, list) or isinstance(what, tuple) or what is None:
            what = ','.join(self.escape(f) for f in what) if what else '*'

        sql_query = "SELECT %s FROM %s" % (what, tablename)
        if where:
            sql_query += " WHERE %s" % where
        if group_by:
            sql_query += " GROUP BY %s" % group_by
        if order_by:
            sql_query += " ORDER BY %s" % order_by
        if limit:
            sql_query += " LIMIT %d, %d" % (offset, limit)
        elif offset:
            sql_query += " LIMIT %d, %d" % (offset, limit)
        self.logger.debug("<sql: %s>" % sql_query)

        for row in self._execute(sql_query, where_values):
            yield row

    def _select2(self, tablename=None, what="*", where_columns=None, where_values=None,
                 group_by=None, order_by=None, offset=0, limit=None):
        if where_values is None:
            where_values = []
        if where_columns is None:
            where_columns = []
        tablename = self.escape(tablename)
        if isinstance(what, list) or isinstance(what, tuple) or what is None:
            what = ','.join(self.escape(f) for f in what) if what else '*'

        sql_query = "SELECT %s FROM %s" % (what, tablename)
        if where_columns:
            _condition_values = " and ".join([
                "%s = %s" % (self.escape(k), self.placeholder) for k in where_columns
            ])
            sql_query += " WHERE %s" % _condition_values
        if group_by:
            sql_query += " GROUP BY %s" % group_by
        if order_by:
            sql_query += " ORDER BY %s" % order_by
        if limit:
            sql_query += " LIMIT %d, %d" % (offset, limit)
        elif offset:
            sql_query += " LIMIT %d, %d" % (offset, limit)
        self.logger.debug("<sql: %s>" % sql_query)

        for row in self._execute(sql_query, where_values):
            yield row

    def insert(self, _mode=INSERT_NORMAL, _tablename=None, **values):
        if _mode not in (INSERT_NORMAL, INSERT_REPLACE, INSERT_IGNORE):
            return None
        _mode = INSERT_MODES[_mode]

        _tablename = self.escape(_tablename)
        if values:
            _keys = ", ".join((self.escape(k) for k in values))
            _values = ", ".join([self.placeholder, ] * len(values))
            sql_query = "%s INTO %s (%s) VALUES (%s)" % (_mode, _tablename, _keys, _values)
        else:
            sql_query = "%s INTO %s DEFAULT VALUES" % (_mode, _tablename)
        self.logger.debug("<sql: %s>", sql_query)

        if values:
            dbcur = self._execute(sql_query, list(itervalues(values)))
        else:
            dbcur = self._execute(sql_query)
        return dbcur.lastrowid if dbcur else 0

    def insert_many_with_dict_list(self, tablename, data, mode=INSERT_IGNORE, batch_size=1000):
        if not data or mode not in (INSERT_NORMAL, INSERT_REPLACE, INSERT_IGNORE):
            return

        mode = INSERT_MODES[mode]
        tablename = self.escape(tablename)
        values = data[0]
        if values:
            _keys = ", ".join((self.escape(k) for k in values))
            _values = ", ".join([self.placeholder, ] * len(values))
            # sql_query = "%s INTO %s (%s) VALUES (%s)" % (mode, tablename, _keys, _values)
            sql_query = "%s INTO %s (%s) VALUES (%s)" % (mode, tablename, _keys, _values)
        else:
            # sql_query = "%s INTO %s DEFAULT VALUES" % (mode, tablename)
            sql_query = "%s INTO %s DEFAULT VALUES" % (mode, tablename)
        # self.logger.debug("<sql: %s>", sql_query)
        # self.logger.debug("%s", tuple(data[0][k] for k in values))

        size = (len(data) + batch_size - 1) // batch_size
        for i in range(size):
            # self.logger.info("{} {}/{}".format(mode, batch_size * (i + 1), len(data)))
            # self._executemany(sql_query, [list(d[k] for k in values) for d in data[i * batch_size:(i + 1) * batch_size]])
            self.begin_transaction()
            self._executemany(sql_query, [list(d[k] for k in values) for d in data[i * batch_size:(i + 1) * batch_size]])
            self.end_transaction()
        return len(data)

    def _update(self, _tablename=None, _where="1=0", _where_values=None, **values):
        if _where_values is None:
            _where_values = []
        _tablename = self.escape(_tablename)
        _key_values = ", ".join([
            "%s = %s" % (self.escape(k), self.placeholder) for k in values
        ])
        sql_query = "UPDATE %s SET %s WHERE %s" % (_tablename, _key_values, _where)
        self.logger.debug("<sql: %s>", sql_query)

        return self._execute(sql_query, list(itervalues(values)) + list(_where_values))

    def _update2(self, _tablename=None, _where_columns=None, _where_values=None, **values):
        if _where_values is None:
            _where_values = []
        if _where_columns is None:
            _where_columns = []
        _tablename = self.escape(_tablename)
        _key_values = ", ".join([
            "%s = %s" % (self.escape(k), self.placeholder) for k in values
        ])
        _condition_values = " and ".join([
            "%s = %s" % (self.escape(k), self.placeholder) for k in _where_columns
        ])
        if _where_columns:
            sql_query = "UPDATE %s SET %s WHERE %s" % (_tablename, _key_values, _condition_values)
            self.logger.debug("<sql: %s>", sql_query)
            return self._execute(sql_query, list(itervalues(values)) + list(_where_values))
        else:
            sql_query = "UPDATE %s SET %s" % (_tablename, _key_values)
            self.logger.debug("<sql: %s>", sql_query)
            return self._execute(sql_query, list(itervalues(values)))

    def _delete(self, tablename=None, where="1=0", where_values=None):
        if where_values is None:
            where_values = []
        tablename = self.escape(tablename)
        sql_query = "DELETE FROM %s" % tablename
        if where:
            sql_query += " WHERE %s" % where
        self.logger.debug("<sql: %s>", sql_query)

        return self._execute(sql_query, where_values)

    def __commit__(self):
        self.conn.commit()

    def __rollback__(self):
        self.conn.rollback()

    def __execute__(self, sql, vals=None):
        try:
            dbcur = self.dbcur
            dbcur.execute(sql, vals)
        except psycopg2.InternalError as e:
            if e.pgerror == 'ERROR:  current transaction is aborted, commands ignored until end of transaction block\n':
                self.__rollback__()
                return self.__execute__(sql, vals)
            raise e
        else:
            return dbcur

    def select(self, tab, query_dict, cols_selected='*', filter_='='):
        cols, vals = zip(*query_dict.items())
        cols_selected = ', '.join(cols_selected)
        expr_where = ' AND '.join(["{} {} %s".format(x, filter_) for x in cols])

        sql = "SELECT {} FROM \"{}\" WHERE {}".format(cols_selected, tab, expr_where)
        return self.__execute__(sql, vals)

        # --- omit ---

    def get(self, tab, query_dict, cols_selected='*', filter_='='):
        # here i need cursor
        self.cursor = self.select(tab, query_dict, cols_selected, filter_)
        if self.cursor.rowcount == 1:
            return self.cursor.fetchone()
        elif self.cursor.rowcount == 0:
            raise self.DoesNotExist("No record found!")
        else:
            raise self.MultipleObjectsReturned("Get {} records! only one excepted.".format(self.cursor.rowcount))


IP_STATUS_UNKNOWN = 0
IP_STATUS_ERRNETWORK = 1  # 网络故障
IP_STATUS_SUCCEED = 2  # 成功
IP_STATUS_EXCEED = 3  # IP重置超限


class NetworkException(Exception):
    pass


class ExceedException(Exception):
    pass


