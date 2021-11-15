import copy
import sys

import nb_log
import pymysql
import paramiko
import typing
from concurrent.futures import ThreadPoolExecutor
from threadpool_executor_shrink_able import BoundedThreadPoolExecutor
from universal_object_pool import ObjectPool, AbstractObject
import time
import decorator_libs
from itertools import chain

from d22d.utils.decorators import print_hz


class TimeoutTransport(paramiko.Transport):
    _active_check_timeout = 100


t = TimeoutTransport(('127.0.0.1', 22))



class PyMysqlOperator(AbstractObject):
    error_type_list_set_not_available = []  # 出了特定类型的错误，可以设置对象已经无效不可用了，不归还到队列里面。

    # error_type_list_set_not_available = [pymysql.err.InterfaceError]

    def __init__(self, host='192.168.6.130', user='root', password='123456', cursorclass=pymysql.cursors.SSDictCursor,
                 autocommit=False, **pymysql_connection_kwargs):
        super().__init__()
        in_params = copy.copy(locals())
        in_params.update(pymysql_connection_kwargs)
        in_params.pop('self')
        in_params.pop('pymysql_connection_kwargs')
        in_params.pop('__class__')
        self.in_params = in_params
        self.is_recon = False
        self.conn = None
        self.reconnect(in_params)
        self.logger = nb_log.get_logger(self.__class__.__name__)

    def reconnect(self, in_params):
        while True:
            try:
                self.conn = pymysql.Connection(**in_params)
                if self.is_recon:
                    print('重新连接成功。。。')
                break
            except pymysql.err.OperationalError as ex:
                if ex.args == (1040, 'ny connections'):
                    print('mysql连接数过多，15秒后自动重试重新连接。。。')
                    time.sleep(15)
                    self.is_recon = True
                else:
                    raise
    """ 下面3个是重写的方法"""

    def clean_up(self):  # 如果一个对象最近30分钟内没被使用，那么对象池会自动将对象摧毁并从池中删除，会自动调用对象的clean_up方法。
        self.conn.close()

    def before_use(self):
        self.cursor = self.conn.cursor()
        self.core_obj = self.cursor  # 这个是为了operator对象自动拥有cursor对象的所有方法。

    def before_back_to_queue(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cursor.close()  # 也可以不要，因为每次的cusor都是不一样的。

    """以下可以自定义其他方法。
    因为设置了self.core_obj = self.cursor ，父类重写了__getattr__,所以此对象自动拥有cursor对象的所有方法,如果是同名同意义的方法不需要一个个重写。
    """

    def execute(self, query, args=None):
        """
        这个execute由于方法名和入参和逻辑与官方一模一样，可以不需要，因为设置了core_obj后，operator对象自动拥有cursor对象的所有方法，可以把这个方法注释了然后测试运行不受影响。
        :param query:
        :param args:
        :return:
        """
        return self.cursor.execute(query, args)


if __name__ == "__main__":
    # local_ctr_mysql = MYSQLController(host='127.0.0.1', port=3306, user='mq', passwd='1234qwer', database='gu')
    # local_execute_auto = local_ctr_mysql.execute_auto
    # print(local_execute_auto("select count(0) as cnt from gu1", return_type=1))  # 第一个字典
    # print(local_execute_auto("select count(0) as cnt from gu1", return_type=2))  # 字典列表
    # print(local_execute_auto("select count(0) as cnt from gu1", return_type=3))  # 字典迭代器
    # print(local_execute_auto("select count(0) as cnt from gu1", return_type=4))  # 第一列第一行的值

    mysql_pool = ObjectPool(object_type=PyMysqlOperator, object_pool_size=1000, object_init_kwargs=dict(
        host='127.0.0.1', port=3306, user='mq', password='1234qwer'))

    iii = 0
    time_l = time.time()


    # @print_hz('test_update', per_cnt_print=1000000, print_func=print)
    # def test_update(i):
    #     return i

    @print_hz('test_update', print_func=print)
    def tes_update(i):
        sql = f'''
                INSERT INTO gu.testu(uname , age , numfff)
            VALUES{','.join(['(%s, %s, %s)'] * 1000)}

            ON DUPLICATE KEY UPDATE
                uname = values(uname),
                age = if(values(age)>age,values(age),age),
                numfff = if(values(numfff)>numfff,values(numfff),numfff);
            '''
        with mysql_pool.get(
                timeout=2) as operator:  # type: typing.Union[PyMysqlOperator,pymysql.cursors.DictCursor] #利于补全
            # print(id(operator.cursor), id(operator.conn))
            aa = ((f'name_{i+ii}', ((i+ii) * 4), 3245) for ii in range(1000))
            try:
                operator.execute(sql, args=list(chain.from_iterable(aa)))
            except pymysql.err.OperationalError as e:
                print(type(e),e.__dict__)
            # print(operator.lastrowid)  # opererator 自动拥有 operator.cursor 的所有方法和属性。 opererator.methodxxx 会自动调用 opererator.cursor.methodxxx


    # operator_global = PyMysqlOperator()
    #
    #
    # def test_update_multi_threads_use_one_conn(i):
    #     """
    #     这个是个错误的例子，多线程运行此函数会疯狂报错,单线程不报错
    #     这个如果运行在多线程同时操作同一个conn，就会疯狂报错。所以要么狠low的使用临时频繁在函数内部每次创建和摧毁mysql连接，要么使用连接池。
    #     :param i:
    #     :return:
    #     """
    #     sql = f'''
    #             INSERT INTO db1.table1(uname ,age)
    #         VALUES(
    #             %s ,
    #             %s)
    #         ON DUPLICATE KEY UPDATE
    #             uname = values(uname),
    #             age = if(values(age)>age,values(age),age);
    #         '''
    #
    #     operator_global.before_use()
    #     print(id(operator_global.cursor), id(operator_global.conn))
    #     operator_global.execute(sql, args=(f'name_{i}', i * 3))
    #     operator_global.cursor.close()
    #     operator_global.conn.commit()

    st = time.time()

    # 单线程 平均/峰值 [21.11/24.49] 次/秒
    # for x in range(0, 100):
    #     test_update(x)
    # print(time.time() - st)

    # pool_size 100 32多线程 平均/峰值 [343.59/377.96] 次/秒
    # pool_size 100 100多线程 平均/峰值 [995.77/1067.66] 次/秒
    #  pool_size 1000 220多线程 平均/峰值 [1821.46/1961.02] 次/秒
    #  pool_size 1000 280多线程 平均/峰值 [1732.67/2120.29] 次/秒
    #  pool_size 1000 320多线程 平均/峰值 [2391.82/2617.14] 次/秒
    #  pool_size 1000 320多线程 平均/峰值 [2130.53/2165.50] 次/秒
    #  pool_size 1000 320多线程 批量写1000 2个字段 平均/峰值 [15.64/15.80] 次/秒
    #  pool_size 1000 320多线程 批量写1000 3个字段 平均/峰值 [15.64/15.80] 次/秒
    #  pool_size 1000 400多线程 平均/峰值 [2204.73/2463.14] 次/秒
    #  pool_size 1000 640多线程 平均/峰值 [2231.81/2246.61] 次/秒
    # thread_pool = ThreadPoolExecutor(320)
    thread_pool = BoundedThreadPoolExecutor(320)
    with decorator_libs.TimerContextManager():
        for x in range(200000, 220000):
            thread_pool.submit(tes_update, x)
            # thread_pool.submit(test_update_multi_threads_use_one_conn, x)
    print(1231231111111111111111111111111111)
    #     thread_pool.shutdown()
    # time.sleep(10000)  # 这个可以测试验证，此对象池会自动摧毁连接如果闲置时间太长，会自动摧毁对象
