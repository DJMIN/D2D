# coding=utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sqlparse
import csv
import re
from sqlparse.sql import Identifier, IdentifierList

from sqlparse.tokens import Keyword, Name

RESULT_OPERATIONS = {'UNION', 'INTERSECT', 'EXCEPT', 'SELECT'}
ON_KEYWORD = 'ON'
PRECEDES_TABLE_NAME = {'FROM', 'JOIN', 'DESC', 'DESCRIBE', 'WITH'}


class BaseExtractor(object):
    def __init__(self, sql_statement):

        # TODO format速度慢一倍
        # self.sql = sqlparse.format(sql_statement, reindent=True, keyword_case='upper')
        self.sql = sql_statement
        self._table_names = set()
        self._alias_names = set()
        self._limit = None

        import time
        time_start = time.time()
        self.parsed = sqlparse.parse(self.stripped())
        # for statement in self.parsed:
        #     self.__extract_from_token(statement)
        #     self._limit = self._extract_limit_from_query(statement)
        self._table_names = self._table_names - self._alias_names
        self.sql_type = None
        self.table_name = None
        self.keys = []
        self.values = []

    def get_v_main(self, keys=None):
        import copy
        # self.get_v(self.parsed[0])
        self.get_v_v2(self.parsed[0])
        data = []
        ks = keys or self.keys
        len_k = len(ks)
        # len_v = len(self.values)
        # print(len_k, len_v, ks)
        # print(self.values)
        d_tmp = dict()
        for idx, v in enumerate(self.values):
            if v['type'] == 'Integer':
                res = int(v['value'])
            # if v['type'] == 'Single':
            #     res = v['value']
            elif v['type'] == 'Keyword' and v['value'] == 'NULL':
                res = None
            else:
                val = v['value']
                if val.startswith("'") or val.startswith('"'):
                    val = val[1:]
                if val.endswith("'") or val.endswith('"'):
                    val = val[:-1]
                res = val

            d_tmp[ks[idx % len_k]] = res
            if d_tmp and idx and not (idx + 1) % len_k:
                data.append(copy.deepcopy(d_tmp))
                d_tmp = dict()
        # print(data)
        return data

    @property
    def tables(self):
        return self._table_names

    @property
    def limit(self):
        return self._limit

    def is_select(self):
        return self.parsed[0].get_type() == 'SELECT'

    def get_main_parsed_type(self):
        return self.parsed[0].get_type()

    def is_explain(self):
        return self.stripped().upper().startswith('EXPLAIN')

    def is_readonly(self):
        return self.is_select() or self.is_explain()

    def stripped(self):
        return self.sql.strip(' \t\n;')

    def get_statements(self):
        statements = []
        for statement in self.parsed:
            if statement:
                sql = str(statement).strip(' \n;\t')
                if sql:
                    statements.append(sql)
        return statements

    @staticmethod
    def __precedes_table_name(token_value):
        for keyword in PRECEDES_TABLE_NAME:
            if keyword in token_value:
                return True
        return False

    @staticmethod
    def get_full_name(identifier):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            return '{}.{}'.format(identifier.tokens[0].value,
                                  identifier.tokens[2].value)
        return identifier.get_real_name()

    @staticmethod
    def __is_result_operation(keyword):
        for operation in RESULT_OPERATIONS:
            if operation in keyword.upper():
                return True
        return False

    @staticmethod
    def __is_identifier(token):
        return isinstance(token, (IdentifierList, Identifier))

    def __process_identifier(self, identifier):
        if '(' not in '{}'.format(identifier):
            self._table_names.add(self.get_full_name(identifier))
            return

        # store aliases
        if hasattr(identifier, 'get_alias'):
            self._alias_names.add(identifier.get_alias())
        if hasattr(identifier, 'tokens'):
            # some aliases are not parsed properly
            if identifier.tokens[0].ttype == Name:
                self._alias_names.add(identifier.tokens[0].value)
        self.__extract_from_token(identifier)

    def as_create_table(self, table_name, overwrite=False):
        exec_sql = ''
        sql = self.stripped()
        if overwrite:
            exec_sql = 'DROP TABLE IF EXISTS {};\n'.format(table_name)
        exec_sql += 'CREATE TABLE {} AS \n{}'.format(table_name, sql)
        return exec_sql

    def __extract_from_token(self, token):
        if not hasattr(token, 'tokens'):
            return

        table_name_preceding_token = False

        for item in token.tokens:
            if item.is_group and not self.__is_identifier(item):
                self.__extract_from_token(item)

            if item.ttype in Keyword:
                if self.__precedes_table_name(item.value.upper()):
                    table_name_preceding_token = True
                    continue

            if not table_name_preceding_token:
                continue

            if item.ttype in Keyword or item.value == ',':
                if (self.__is_result_operation(item.value) or
                        item.value.upper() == ON_KEYWORD):
                    table_name_preceding_token = False
                    continue
                # FROM clause is over
                break

            if isinstance(item, Identifier):
                self.__process_identifier(item)

            if isinstance(item, IdentifierList):
                for token in item.tokens:
                    if self.__is_identifier(token):
                        self.__process_identifier(token)

    def _get_limit_from_token(self, token):
        if token.ttype == sqlparse.tokens.Literal.Number.Integer:
            return int(token.value)
        elif token.is_group:
            return int(token.get_token_at_offset(1).value)

    def _extract_limit_from_query(self, statement):
        limit_token = None
        for pos, item in enumerate(statement.tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_token = statement.tokens[pos + 2]
                return self._get_limit_from_token(limit_token)

    def get_query_with_new_limit(self, new_limit):
        if not self._limit:
            return self.sql + ' LIMIT ' + str(new_limit)
        limit_pos = None
        tokens = self.parsed[0].tokens
        # Add all items to before_str until there is a limit
        for pos, item in enumerate(tokens):
            if item.ttype in Keyword and item.value.lower() == 'limit':
                limit_pos = pos
                break
        limit = tokens[limit_pos + 2]
        if limit.ttype == sqlparse.tokens.Literal.Number.Integer:
            tokens[limit_pos + 2].value = new_limit
        elif limit.is_group:
            tokens[limit_pos + 2].value = (
                '{}, {}'.format(next(limit.get_identifiers()), new_limit)
            )

        str_res = ''
        for i in tokens:
            str_res += str(i.value)
        return str_res

    def get_v(self, tok):
        for token in tok.tokens:
            if token._get_repr_name() in 'Parenthesis':
                # print("ParenthesisParenthesisParenthesisParenthesis", token)
                self.get_v(token)
            elif token._get_repr_name() == 'Values':
                # print("ValuesValuesValuesValues", token)
                self.get_v(token)
            elif token._get_repr_name() == 'IdentifierList':
                # print("IdentifierListIdentifierListIdentifierListIdentifierList", token)
                self.get_v(token)
            elif token._get_repr_name() in ('Punctuation', 'Whitespace', 'Newline'):
                pass
            elif token._get_repr_name() in ('Keyword',) and token.value != 'NULL':
                pass
            elif token._get_repr_name() == 'DML':
                self.sql_type = token._get_repr_value()
            elif token._get_repr_name() == 'Identifier':
                self.get_v(token)
            elif token._get_repr_name() == 'Name':
                if token.value.startswith('`'):
                    if self.table_name:
                        # self.keys.append({'type': token._get_repr_name(), 'value': token.value.replace('`', ''), 'last': tok._get_repr_name()})
                        self.keys.append(token.value.replace('`', ''))
                    else:
                        self.table_name = token.value.replace('`', '')
            elif token._get_repr_name() == 'Function':
                self.get_v(token)
            else:
                self.values.append({'type': token._get_repr_name(), 'value': token.value})
                # pprint_dir(tokens)

    def get_v_v2(self, tok):
        for token in tok.flatten():
            if token._get_repr_name() in ('Parenthesis', 'Values', 'IdentifierList'):
                pass
            elif token._get_repr_name() in ('Punctuation', 'Whitespace', 'Newline'):
                pass
            elif token._get_repr_name() in ('Keyword',) and token.value != 'NULL':
                pass
            elif token._get_repr_name() == 'DML':
                self.sql_type = token._get_repr_value()
            elif token._get_repr_name() == 'Identifier':
                pass
            elif token._get_repr_name() == 'Name':
                if token.value.startswith('`'):
                    if self.table_name:
                        # self.keys.append({'type': token._get_repr_name(), 'value': token.value.replace('`', ''), 'last': tok._get_repr_name()})
                        self.keys.append(token.value.replace('`', ''))
                    else:
                        self.table_name = token.value.replace('`', '')
            elif token._get_repr_name() == 'Function':
                pass
            else:
                self.values.append({'type': token._get_repr_name(), 'value': token.value})


class SqlExtractor(BaseExtractor):
    """提取sql语句"""

    @staticmethod
    def get_full_name(identifier, including_dbs=False):
        if len(identifier.tokens) > 1 and identifier.tokens[1].value == '.':
            a = identifier.tokens[0].value
            b = identifier.tokens[2].value
            db_table = (a, b)
            full_tree = '{}.{}'.format(a, b)
            if len(identifier.tokens) == 3:
                return full_tree
            else:
                i = identifier.tokens[3].value
                c = identifier.tokens[4].value
                if i == ' ':
                    return full_tree
                full_tree = '{}.{}.{}'.format(a, b, c)
                return full_tree
        return None, None

    sql_type = None
    keys = []
    values = []
    data = []
    table_name = None


import sys
import os
import io


class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = io.StringIO()
        self.path = os.path.abspath(os.path.dirname(__file__))
        self.type = sys.getfilesystemencoding()

    def write(self, message):
        # self.terminal.write(message)
        # print(message)
        self.log.write(message)
        self.log.readable()

    def flush(self):
        self.log.flush()


def pprint_dir(data, get_attr='', skip='__', print_func=True, run_func=True, print_attr=True):
    """
    获取一个参数的所有属性 以及方法 的值 打印出来

    get_attr: 尝试获取单个属性
    """

    print(f'1111111111111111111111111 pprint_dir 1111111111111111111111111        '
          f'[ type: {str(type(data))[8:-2]:25s} ]      [ __repr__: {data.__repr__()}] {data.__str__().__repr__()}')
    for attr in dir(data):
        if skip and attr.startswith(skip):
            continue
        if get_attr and get_attr != attr:
            continue
        d_attr = getattr(data, attr)

        if callable(d_attr):
            if not print_func:
                continue
            if run_func:
                old = sys.stdout
                try:
                    tmp = Logger()
                    sys.stdout = tmp
                    res = d_attr()
                    sys.stdout = old
                    value = f'[   res][ rtype: {str(type(res))[8:-2]:65s}] {res}   |  {tmp.log.getvalue()}'
                    # value = f'[funcR][{str(type(res))[8:-2]:25}] {res}'
                except Exception as ex:
                    value = f'[except][ etype: {str(type(ex))[8:-2]:65s}] {ex}'
                finally:
                    sys.stdout = old
            else:
                value = '[  func][  fdoc: {:65s}] {}'.format((d_attr.__doc__ or '').split("\n")[0], d_attr)
        else:
            if not print_attr:
                continue
            value = f'[  attr][ atype: {str(type(d_attr))[8:-2]:65s}] {d_attr}'
        print('[{:25s}]{}'.format(attr, value.replace("\n", "\\n")))

    print(f'2222222222222222222222222 pprint_dir 2222222222222222222222222        '
          f'[ type: {str(type(data))[8:-2]:25s} ]      [ __repr__: {data.__repr__()}] {data.__str__().__repr__()}\n')


def main111():
    with open('/home/user/Desktop/test.sql', 'r', encoding='utf-8') as f:
        table_info_str = ''
        flag_create_table = False
        table_keys = []

        def is_insert(li):
            return li.lower()[:7] in ('insert ', 'replace')

        def get_table_keys(table_info_str_raw):
            if table_info_str_raw.endswith(';'):
                table_info_str_raw = table_info_str_raw[-1]
            info_str = f'{table_info_str_raw});'
            return list(SqlExtractor(info_str).get_v_main()[0].keys())

        for line in f:
            line = line.strip()
            if line.lower().startswith('create table'):
                flag_create_table = True
            elif line.lower() == '-- ----------------------------':
                flag_create_table = False
                if table_info_str and not table_keys:
                    table_keys = get_table_keys(table_info_str)
            elif is_insert(line):
                flag_create_table = False
                if table_info_str and not table_keys:
                    table_keys = get_table_keys(table_info_str)
            if flag_create_table:
                if line.strip().startswith('`') or line.strip().lower().startswith('create table'):
                    table_info_str += line
            if is_insert(line):
                yield from match_insert(line)
                # for data in SqlExtractor(line.strip()).get_v_main(table_keys):
                #     yield data
                # 在正则表达式最前面加上 (?i) 就可以忽略后面所有的大小写


RANDOM_STR = '~!}#:~!|F"SF:}~!@WFR{:FE!@#R#3p[r23kef[1p2343213vdgbv21331r123rfefe2g23geb|}}{}{@!{}djk(#JFD)P!#!LSFD"21r1rff43>F{:DS'


def format_col(col):
    if len(col) == 0:
        return ''
    elif col == 'NULL':
        return None
    elif isinstance(col, str):
        col = col.replace("\\'", "'").replace(RANDOM_STR, "\\")
        if col.startswith("'") and col.endswith("'"):
            col = col[1:-1]
        elif not re.sub('[0-9]', '', col):
            col = int(col)
        return col
    else:
        return col


def parse_values(values):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """

    delimiter = ', '
    quotechar = "'"
    latest_row = []
    reader = csv.reader(
        [values.replace("\\\\", RANDOM_STR).replace("\\", "\\\\").replace('\\"', f'{RANDOM_STR}"')],
        # v:= [values.replace("\\'", "\\\\'")],
        # [values.replace("\\'", random_str)],
        # [values],
        delimiter=',',
        doublequote=False,
        # skipinitialspace=True,
        escapechar='\\',
        quotechar=quotechar,
        strict=True
    )
    # reader = re.split(f"{delimiter}(?=([^{quotechar}]*{quotechar}[^{quotechar}]*{quotechar})*[^{quotechar}]*$)", values)
    # reader = re.findall(f'[^{delimiter}{quotechar}]+|{delimiter}{delimiter}|(?:{quotechar}[^{delimiter}{quotechar}]*{quotechar}[^{quotechar}]*{quotechar}[^{quotechar}]*){quotechar}|{quotechar}(?:[^{quotechar}])*{quotechar}', values)
    last_col = ''
    last_split = False
    # reader = re.split(f'(?:^|({delimiter}))(?:{quotechar}(([^{quotechar}]*)+((?:{quotechar}{quotechar}([^{quotechar}]*)+)*)+){quotechar}|(((^((?!{delimiter}|{quotechar}).)+$)*)+))', values)
    # for i in range(1):
    #     for column in reader:

    for reader_row in reader:
        for idx, column in enumerate(reader_row):
            if column == f"{delimiter}{quotechar}":
                last_col += ",'"
                latest_row.append(format_col(last_col))
                latest_row.append('')
                last_col = ""
                column = ""
            if column.startswith(f"{delimiter}"):
                last_col += ",'"
                latest_row.append(format_col(last_col))
                last_col = "'"
                last_split = True
                column = column[2:]
            if len(column) and column[0] == ' ' and not last_col:
                column = column[1:]
            if not last_col and (not column or delimiter == column):
                continue
            # column = column.strip()
            # If our current string is empty...
            if len(column) == 0 and not last_col:
                latest_row.append('')
                continue
            elif column == 'NULL':
                latest_row.append(None)
                continue
            # If our string starts with an open paren
            if column and column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    # print(latest_row)
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = format_col(latest_row[-1][:-1])
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    yield latest_row
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.

            now_col = format_col(column)
            if (
                    (not last_col) and
                    column.startswith("'") and
                    (
                            (
                                    (
                                            not (
                                                    (column[-1:] == "'" and column[-2:-1] != '\\') or
                                                    (column.endswith("')") and column[-3:-2] != '\\')
                                            )
                                    )
                            ) or
                            len(column) == 1
                    )
            ):
                last_col = column
            elif last_col:
                if column != delimiter:
                    if last_split:
                        last_col += f'{column}'
                    else:
                        last_col += f',{column}'
                    last_split = False
                else:
                    last_col += delimiter
                    last_split = True
                if column[-1:] == "'" and column[-2:-1] != '\\':
                    latest_row.append(format_col(last_col))
                    last_col = ''
                elif column.endswith("')") and column[-3:-2] != '\\':
                    latest_row.append(last_col)
                    last_col = ''
            else:
                latest_row.append(now_col)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row and latest_row[-1]:
            # if latest_row[-1][-2:] == ");":
            #     latest_row[-1] = format_col(latest_row[-1][:-2])
            #     yield latest_row
            if latest_row[-1][-1:] == ")":
                latest_row[-1] = format_col(latest_row[-1][:-1])
                yield latest_row


def match_insert(line):
    m = re.fullmatch(
        f'(?i)(INSERT|REPLACE|INSERT IGNORE) INTO\s?`?(?P<table_name>.+?)`?\s?(\((?P<keys>.+?)\)\s)?VALUES\s?(?P<values>(\(.+\)[\s,]?)+);?',
        line)
    table_name, keys, values = '', [], []
    if m:
        table_name = m.group('table_name')
        keys = [re.sub(r'[`\'\"\s]', '', key) for key in m.group('keys').strip().split(',')] if m.group('keys') else []
        # print([key for key in re.split(r'\),\s?\(', m.group('values').strip())])
        # print(m.group('values').strip())
        values = [_ for _ in parse_values(m.group('values').strip())]

    return table_name, keys, values


if __name__ == '__main__':

    # for i in parse_values(r"""(503802367, NULL, '(｡•ˇ‸ˇ•｡) 为中华之崛起添砖加瓦', '❤️', 'lovefeng')"""):
    # for i in parse_values(r"""(1418913438, NULL, 'faku', 'gh6g,, ,,,', '')"""):
    # for i in parse_values(r"""(1023904092, NULL, 'JIANG,', 'Ben', '')"""):
    for i in parse_values(r"""(2, 1, NULL, b'0000000000000000000000000000000000000000000000000000000000000001', NULL, '33', '2020-12-09', '2020-12-09 17:52:44.000000', 234, 2134, '1222', 123, NULL, NULL, 234, 13, '{\"1\": \"2\"}', NULL, NULL, '234', NULL, 234, '123r', NULL, NULL, NULL, 1234, NULL, NULL, 1234, '', 1234, '1234', '17:57:22.000000', '2020-12-09 17:57:25.000000', NULL, 1, 'd213d', NULL, '23r2e132r1')"""):
    # for i in parse_values(r"""(1023904092, NULL, 'JIANG,'', 'Ben', '')"""):
    # for i in parse_values(r"""(952012376, NULL, 'Remy', '\" 孙琪\"', '')"""):
        print(i)

    # sql = """select
    # b.product_name "产品",
    # count(a.order_id) "订单量",
    # b.selling_price_max "销售价",
    # b.gross_profit_rate_max/100 "毛利率",
    # case when b.business_type =1 then '自营消化' when b.business_type =2 then '服务商消化'  end "消化模式"
    # from(select 'CRM签单' label,date(d.update_ymd) close_ymd,c.product_name,c.product_id,
    #     a.order_id,cast(a.recipient_amount as double) amt,d.cost
    #     from mysql4.dataview_fenxiao.fx_order a
    #     left join mysql4.dataview_fenxiao.fx_order_task b on a.order_id = b.order_id
    #     left join mysql7.dataview_trade.ddc_product_info c on cast(c.product_id as varchar) = a.product_ids and c.snapshot_version = 'SELLING'
    #     inner join (select t1.par_order_id,max(t1.update_ymd) update_ymd,
    #                 sum(case when t4.product2_type = 1 and t5.shop_id is not null then t5.price else t1.order_hosted_price end) cost
    #                from hive.bdc_dwd.dw_mk_order t1
    #                left join hive.bdc_dwd.dw_mk_order_status t2 on t1.order_id = t2.order_id and t2.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #                left join mysql7.dataview_trade.mk_order_merchant t3 on t1.order_id = t3.order_id
    #                left join mysql7.dataview_trade.ddc_product_info t4 on t4.product_id = t3.MERCHANT_ID and t4.snapshot_version = 'SELLING'
    #                left join mysql4.dataview_scrm.sc_tprc_product_info t5 on t5.product_id = t4.product_id and t5.shop_id = t1.seller_id
    #                where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #                and t2.valid_state in (100,200) ------有效订单
    #                and t1.order_mode = 10    --------产品消耗订单
    #                and t2.complete_state = 1  -----订单已经完成
    #                group by t1.par_order_id
    #     ) d on d.par_order_id  = b.task_order_id
    #     where c.product_type = 0 and date(from_unixtime(a.last_recipient_time)) > date('2016-01-01') and a.payee_type <> 1 -----------已收款
    #     UNION ALL
    #     select '企业管家消耗' label,date(c.update_ymd) close_ymd,b.product_name,b.product_id,
    #     a.task_id,(case when a.yb_price = 0 and b.product2_type = 1 then b.selling_price_min else a.yb_price end) amt,
    #     (case when a.yb_price = 0 and b.product2_type = 2 then 0 when b.product2_type = 1 and e.shop_id is not null then e.price else c.order_hosted_price end) cost
    #     from mysql8.dataview_tprc.tprc_task a
    #     left join mysql7.dataview_trade.ddc_product_info b on a.product_id = b.product_id and b.snapshot_version = 'SELLING'
    #     inner join hive.bdc_dwd.dw_mk_order c on a.order_id = c.order_id and c.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #     left join hive.bdc_dwd.dw_mk_order_status d on d.order_id = c.order_id and d.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #     left join mysql4.dataview_scrm.sc_tprc_product_info e on e.product_id = b.product_id and e.shop_id = c.seller_id
    #     where  d.valid_state in (100,200) and d.complete_state = 1  and c.order_mode = 10
    #     union ALL
    #     select '交易管理系统' label,date(t6.close_ymd) close_ymd,t4.product_name,t4.product_id,
    #     t1.order_id,(t1.order_hosted_price-t1.order_refund_price) amt,
    #     (case when t1.order_mode <> 11 then t7.user_amount when t1.order_mode = 11 and t4.product2_type = 1 and t5.shop_id is not null then t5.price else t8.cost end) cost
    #     from hive.bdc_dwd.dw_mk_order t1
    #     left join hive.bdc_dwd.dw_mk_order_business t2 on t1.order_id = t2.order_id and t2.acct_day=substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #     left join mysql7.dataview_trade.mk_order_merchant t3 on t1.order_id = t3.order_id
    #     left join mysql7.dataview_trade.ddc_product_info t4 on t4.product_id = t3.MERCHANT_ID and t4.snapshot_version = 'SELLING'
    #     left join mysql4.dataview_scrm.sc_tprc_product_info t5 on t5.product_id = t4.product_id and t5.shop_id = t1.seller_id
    #     left join hive.bdc_dwd.dw_fact_task_ss_daily t6 on t6.task_id = t2.task_id and t6.acct_time=date_format(date_add('day',-1,current_date),'%Y-%m-%d')
    #     left join (select a.task_id,sum(a.user_amount) user_amount
    #                from hive.bdc_dwd.dw_fn_deal_asyn_order a
    #                where a.is_new=1 and a.service='Trade_Payment' and a.state=1 and a.acct_day=substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #                group by a.task_id)t7 on t7.task_id = t2.task_id
    #     left join (select t1.par_order_id,sum(t1.order_hosted_price - t1.order_refund_price) cost
    #                from hive.bdc_dwd.dw_mk_order t1
    #                where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2) and t1.order_type = 1 and t1.order_stype = 4 and t1.order_mode = 12
    #                group by t1.par_order_id) t8 on t1.order_id = t8.par_order_id
    #     where t1.acct_day = substring(cast(DATE_ADD('day',-1,CURRENT_DATE) as varchar),9,2)
    #     and t1.order_type = 1 and t1.order_stype in (4,5) and t1.order_mode <> 12 and t4.product_id is not null and t1.order_hosted_price > 0 and t6.is_deal = 1 and t6.close_ymd >= '2018-12-31'
    # )a
    # left join mysql7.dataview_trade.ddc_product_info b on a.product_id = b.product_id and b.snapshot_version = 'SELLING'
    # where b.product2_type = 1 -------标品
    # and close_ymd between DATE_ADD('day',-7,CURRENT_DATE)  and DATE_ADD('day',-1,CURRENT_DATE)
    # GROUP BY b.product_name,
    # b.selling_price_max,
    # b.gross_profit_rate_max/100,
    # b.actrul_supply_num,
    # case when b.business_type =1 then '自营消化' when b.business_type =2 then '服务商消化'  end
    # order by count(a.order_id) desc
    # limit 10"""
    #
    # sql = '''
    # INSERT INTO `chat_media`(`fromid`, `toidfix`, `msgid`, `toid`, `dialogid`, `clientid`, `chatid`, `totype`, `msgtype`, `size`, `realsize`, `timeupdate`, `dateint`, `date`, `grouped_id`, `md5`, `url`, `display_url`, `status`, `message`, `obj`, `filename`, `savepath`, `error`, `location`, `timeupload`) VALUES (0, -1001401435758, 2466, 1401435758, '1401435758', 1250199136, 1401435758, 'PeerChannel', 'PhotoVideo', 92371, 78702, 1605035384, 1589634510, '2020-05-16 13:08:30.000000', NULL, NULL, NULL, NULL, 3, NULL, 0x80049566040000000000007D94288C015F948C074D657373616765948C026964944DA2098C05746F5F6964947D942868018C0B506565724368616E6E656C948C0A6368616E6E656C5F6964944A6E368853758C0464617465948C086461746574696D65948C086461746574696D65949394430A07E405100D081E0000009468098C0874696D657A6F6E6594939468098C0974696D6564656C74619493944B004B004B008794529485945294869452948C076D657373616765948C00948C036F757494898C096D656E74696F6E656494898C0C6D656469615F756E7265616494898C0673696C656E7494898C04706F737494888C0E66726F6D5F7363686564756C656494898C066C656761637994898C09656469745F6869646594898C0766726F6D5F6964944E8C086677645F66726F6D944E8C0A7669615F626F745F6964944E8C0F7265706C795F746F5F6D73675F6964944E8C056D65646961947D942868018C114D6573736167654D6564696150686F746F948C0570686F746F947D942868018C0550686F746F9468038A0884AA311B55EA00568C0B6163636573735F68617368948A08E7D954740E9BFE498C0E66696C655F7265666572656E636594431D025388366E000009A25FAAE56E43FF7817813470101B929673DB7F095E946808680B430A07E405100D081E000000946814869452948C0573697A6573945D94287D942868018C1150686F746F537472697070656453697A65948C0474797065948C0169948C0562797465739443B901281C2592759D963190B8383DC55A122C8AAEB8DA7D4536310CEC55872876E7A517DB45AFA0CE33E94AFA8EC4A18872BD88C8A50FEA2A8C3310989DB1229E33E9560CE78DB823154B524A0A76EF2AC724F193EF5A2C3CC8809594211CFBD662B0C14738C5598D89B19D5096555EFDAA0B1B3DABC68E550F963A1CE6A282798478400A83C6696D65B8401811B3B21AB2A2193E621E227AAAF2298AE46F023F2473524319104B1C6A3E75A28A484468BFBB5FA0A7E3145140CF94757D942868018C0950686F746F53697A659468358C016D948C086C6F636174696F6E947D942868018C1A46696C654C6F636174696F6E546F426544657072656361746564948C09766F6C756D655F6964948A05B765C06A748C086C6F63616C5F6964944AE70A0200758C0177944BE28C0168944D40018C0473697A65944D7D54757D94286801683A68358C017894683C7D94286801683E683F8A05B765C06A7468404AE80A02007568414D360268424D200368434AD3680100757D94286801683A68358C017994683C7D94286801683E683F8A05B765C06A7468404AE50A02007568414DBC0268424DDD0368434A6E33010075658C0564635F6964944B058C0C6861735F737469636B6572739489758C0B74746C5F7365636F6E6473944E758C0C7265706C795F6D61726B7570944E8C08656E746974696573945D948C057669657773944B048C09656469745F64617465944E8C0B706F73745F617574686F72944E8C0A67726F757065645F6964944E8C127265737472696374696F6E5F726561736F6E945D94752E, 'None.jpg', 'PhotoVideo/1401435758/2466--2020-05-16T21_08_30+08_00--None.jpg', NULL, NULL, 1606712817);
    # '''
    sql = '''
    /*
 Navicat Premium Data Transfer

 Source Server         : local
 Source Server Type    : MySQL
 Source Server Version : 50727
 Source Host           : localhost:3306
 Source Schema         : 122_

 Target Server Type    : MySQL
 Target Server Version : 50727
 File Encoding         : 65001

 Date: 09/12/2020 17:58:13
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for test
-- ----------------------------
DROP TABLE IF EXISTS `test`;
CREATE TABLE `test`  (
  `id` bigint(64) NOT NULL AUTO_INCREMENT,
  `bigint` bigint(255) NULL DEFAULT NULL,
  `binary` binary(255) NULL DEFAULT NULL,
  `bit` bit(64) NULL DEFAULT NULL,
  `blob` blob NULL,
  `char` char(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL DEFAULT NULL,
  `date` date NULL DEFAULT NULL,
  `datetime` datetime(6) NULL DEFAULT NULL,
  `decimal` decimal(65, 0) NULL DEFAULT NULL,
  `double` double(255, 0) NULL DEFAULT NULL,
  `enum` enum('123','1222') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL DEFAULT NULL,
  `float` float(255, 0) NULL DEFAULT NULL,
  `geometry` geometry NULL,
  `geometrycollection` geometrycollection NULL,
  `int` int(255) NULL DEFAULT NULL,
  `integer` int(255) NULL DEFAULT NULL,
  `json` json NULL,
  `linestring` linestring NULL,
  `longblob` longblob NULL,
  `longtext` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  `mediumblob` mediumblob NULL,
  `mediumint` mediumint(255) NULL DEFAULT NULL,
  `mediumtext` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  `multilinestring` multilinestring NULL,
  `multipoint` multipoint NULL,
  `multipolygon` multipolygon NULL,
  `numeric` decimal(65, 0) NULL DEFAULT NULL,
  `point` point NULL,
  `polygon` polygon NULL,
  `real` double(255, 0) NULL DEFAULT NULL,
  `set` set('') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL DEFAULT '',
  `smallint` smallint(255) NULL DEFAULT NULL,
  `text` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  `time` time(6) NULL DEFAULT NULL,
  `timestamp` timestamp(6) NULL DEFAULT NULL,
  `tinyblob` tinyblob NULL,
  `tinyint` tinyint(255) NULL DEFAULT NULL,
  `tinytext` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL,
  `varbinary` varbinary(255) NULL DEFAULT NULL,
  `varchar` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_520_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of test
-- ----------------------------
INSERT INTO `test` VALUES (1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO `test` VALUES (2, 1, NULL, b'0000000000000000000000000000000000000000000000000000000000000001', NULL, '33', '2020-12-09', '2020-12-09 17:52:44.000000', 234, 2341, '1222', 23, NULL, NULL, 234, 13, '{\"1\": \"2\"}', NULL, NULL, '234', NULL, 234, '123r', NULL, NULL, NULL, 1234, NULL, NULL, 1234, '', 1234, '1234', '17:57:22.000000', '2020-12-09 17:57:25.000000', NULL, 1, 'd213d', NULL, '23r2e132r1'),(2, 1, NULL, b'0000000000000000000000000000000000000000000000000000000000000001', NULL, '33', '2020-12-09', '2020-12-09 17:52:44.000000', 234, 2341, '1222', 23, NULL, NULL, 234, 13, '{\"1\": \"2\"}', NULL, NULL, '234', NULL, 234, '123r', NULL, NULL, NULL, 1234, NULL, NULL, 1234, '', 1234, '1234', '17:57:22.000000', '2020-12-09 17:57:25.000000', NULL, 1, 'd213d', NULL, '6566666(2, 1, NULL, b'0000000000000000000000000000000000000000000000000000000000000001', NULL, '33', '2020-12-09', '2020-12-09 17:52:44.000000', 234, 2341, '1222', 23, NULL, NULL, 234, 13, '{\"1\": \"2\"}', NULL, NULL, '234', NULL, 234, '123r', NULL, NULL, NULL, 1234, NULL, NULL, 1234, '', 1234, '1234', '17:57:22.000000', '2020-12-09 17:57:25.000000', NULL, 1, 'd213d', NULL, '23r2e132r1'),(2, 1, NULL, b'0000000000000000000000000000000000000000000000000000000000000001', NULL, '33', '2020-12-09', '2020-12-09 17:52:44.000000', 234, 2341, '1222', 23, NULL, NULL, 234, 13, '{\"1\": \"2\"}', NULL, NULL, '234', NULL, 234, '123r', NULL, NULL, NULL, 1234, NULL, NULL, 1234, '', 1234, '1234', '17:57:22.000000', '2020-12-09 17:57:25.000000', NULL, 1, 'd213d', NULL, '434311111111111111111111111111112')');

SET FOREIGN_KEY_CHECKS = 1;
    '''
    # sql_extractor = SqlExtractor(sql)
    import random

    # print(sql_extractor.sql)

    # sql_extractor.get_v(sxql_extractor.parsed[0].tokens)
