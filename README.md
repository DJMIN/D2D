# d22d数据库迁移

# D22[twaɪs]D

```
from mysql .csv .sql .json .xls .xlsx ElasticSearch excel mongodb sqlite redis clickhouse 
to mysql .csv .sql .json .xls .xlsx ElasticSearch excel mongodb sqlite redis clickhouse 
```

用非常简单的代码进行数据ETL
可以全量迁移上亿以上单表数据
或者依据两个表各自主键合并两个表，生成一个新表
2行代码即可使用最高效的方式最快的速度在各种数据库中流式处理迁移单表数据，不需要操心内存问题，错误自动重试，并且可以自定义每一行每个字段的处理函数，处理得到新的数据，实行计划任务，定时迁移增量数据
可以根据数据自动建库建表，而无需操心数据格式和类型
拥有详细的日志记录

支持.csv .sql .json .xls .xlsx mysql ElasticSearch excel mongodb sqlite redis...表到表互相转换

```text
ETL with simple code and easy code，   
Migrating form DataBase to DataBase by 2 lines code, The fastest migration tool for all database by scheduled tasks with
2 lines of code.  
Can automatically build a table based on data  
Has detailed logging  
```

ETL是将业务系统的数据经过抽取（Extract）、清洗转换（Transform）之后加载（Load）到数据仓库的过程，
目的是将企业中的分散、零乱、标准不统一的数据整合到一起，为企业的决策提供分析依据。

#### ETL全流程 DEMO

```python
from d22d import (MySqlD, Migration)
Migration(
    database_from=MySqlD(host='localhost', port=3306, database='test',user='root', passwd='root'),  # 数据来源数据库
    database_to=MySqlD(host='192.168.0.100', port=3307, database='dbtest', user='root', passwd='root'),  # 数据去向数据库
    table_from='user_test',  # 数据来源数据库表名
    table_to='user'  # 数据去向数据库表名
).run() # 省略字段处理，一行代码一步到位
```

---

#### 抽取（Extract）

    MySqlD(host='192.168.0.100', port=3306, database='test', user='root', passwd='root').get_data()

#### 清洗转换（Transform）

    def self_format(data):
        # 重写Migration的format_data函数，自定义每一行如何用代码处理
        new_row = data
        new_row['uuid'] = data.pop('user_id')
        new_row['time'] = int(time.time())
        new_row['tag'] = 'xs'
        return new_row
    Migration.format_data = self_format

#### 加载（Load）

    MySqlD(host='192.168.0.100', port=3306, database='test', user='root', passwd='root').save_data()

---

## Install 安装

### 互联网用户：

```shell
pip install d22d
```

### 源码调试用户：

* 以下说明完全只针对安装源码调试的用户，
  互联网用户sudo pip install d22d 已可使用，可跳过本节安装说明直至[&#34;RUN 如何使用&#34;](#trun)

##### 安装python3.7以上

###### python3 centos

```shell
yum install -y wget tar libffi-devel zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gcc make 
wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz
tar -zxvf Python-3.7.0.tgz
cd Python-3.7.0
./configure
make&&make install
```

###### python3 ubuntu

```shell
sudo apt update
sudo apt-get update
sudo apt-get install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install -y python3.8 redis-server python3.8-distutils supervisor python3.8-dev openssl libssl-dev
sudo python3.8 -m pip install six
curl https://bootstrap.pypa.io/get-pip.py | sudo -H python3.8

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
sudo python3.8 get-pip.py 
```

##### 安装python依赖包

    git clone https://github.com/DJMIN/D2D.git
    cd D2D
    pip3.7 install -r requirements.txt

---

### 内网用户

如果是无法连接互联网下载python环境，无法pip install安装python依赖包，这里贴心的为你准备了python虚拟环境包，使用方法：

1.下载整个项目zip包解压到本地

    cd D2D
    vim venv-ubuntu-x64/bin/activate

2.修改venv-ubuntu-x64/bin/activate文件的这一行(应该是第40行)为你项目文件的目录下的D2D/venv-ubuntu-x64路径，这里必须是绝对路经

    VIRTUAL_ENV="/home/user/PycharmProjects/D2D/venv-ubuntu-x64"

（如果是windows则修改：D2D\venv-windows-x64\pyvenv.cfg的第一行为你的python3.7目录）

3.然后回到终端窗口执行命令，如果是linux64位系统则使用venv-ubuntu-x64，windows则使用venv-windows-x64

    source venv-ubuntu-x64/bin/activate

4.最后就可以开开心心

    python run.py

## `<a name="trun">`RUN 如何使用 `</a>`

###### Migration类的定义

参数

```text
database_from： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等   
database_to： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等  
table_from： index名或table名或文件名（如不带则会遍历所有数据库表进行迁移到指定数据库同名表）  
table_to： index名或table名或文件名  
pks：定义表的主键，或者复合主键用“,”分割，默认为空字符串''
pkd：数据库到数据库整个迁移的时候，定义每个表的主键pks，dict型，默认为None
windows：定义流式游标处理读取数据时的窗口大小，默认为10000，相当于每次从数据库中每次取10000条数据到内存里
count_from：当已知来源数据库表读取的总条数时可以给定，以此来节约程序计算数据量大小的时间，否则则会通过命令计算，默认为None，为日志中显示处理数据的百分比进度条而存在
size：定义来源数据库表读取的限制条数，相当于mysql的limit，不过是在内存中处理，默认为None不限制，int型
quchong：定义来源数据库表读取的数据是否需要在内存中用set去重，大数据量慎用，相当于mysql的dis，不过是在内存中处理，默认为None不限制，int型
get_data_kwargs：定义get_data的自定义参数，深度使用用户可能才需要
save_data_kwargs：定义save_data的自定义参数，深度使用用户可能才需要
```

方法

```text
run(): 开始执行ETL流程，失败自动重试，命令行打印进度百分比
format_data(data): 定义每一行数据的处理逻辑，传入dict，返回dict，默认是不变
```

##### CsvD类的定义

参数

```text
path：定义csv文件保存的根路径
split：定义csv文件的分隔符，默认=','
extension：定义csv文件的后缀名，默认='csv'
encoding：定义csv文件的读取编码格式，默认='utf8'
```

方法

```text
get_count(): 获取文件行数
get_data(file_name, fieldnames=None, restkey=None, restval=None, dialect="excel", **kwargs): 流式读取csv文件
save_data(file_name, data, *args, **kwargs): 流式保存csv文件
create_index(file_name, data, *args, **kwargs): 建立csv文件并写入第一行键值（表头）
```

##### XlsxIbyFileD类的定义

参数

```text
path：定义csv文件保存的根路径
extension：定义csv文件的后缀名，默认='xlsx'
```

方法

```text
get_count(file_name): 获取文件行数
get_data(file_name, fieldnames=None, restkey=None, restval=None, dialect="excel", **kwargs): 流式读取csv文件
save_data(file_name, data, *args, **kwargs): 流式保存csv文件
create_index(file_name, data, *args, **kwargs): 建立csv文件并写入第一行键值（表头）
```

##### MySqlD类的定义

参数

```text
host: 定义数据库host
port: 定义数据库port
user: 定义数据库user
passwd: 定义数据库passwd
database: 定义数据库database
```

方法

```text
get_count(sql): 获取数据行数
get_data(sql, **kwargs): 流式读取数据
save_data(table_name, data, **kwargs): 流式保存数据
create_index(table_name, data, **kwargs): 建立数据库表并写入第一行键值（表头）
```

##### JsonListD类的定义

    # TODO 自行看源码或者IDE自动补全

##### SqlFileD类的定义

    # TODO 自行看源码或者IDE自动补全

##### MongoDBD类的定义

    # TODO 自行看源码或者IDE自动补全

##### ElasticSearch类的定义

    # TODO 自行看源码或者IDE自动补全

---

# 例子 DEMO

1. need python3
2. create your .py file

```python
import d22d
from d22d import (
 ElasticSearchD, 
 MySqlD,
 CsvD,
 SqlFileD,
 JsonListD,
 XlsIbyFileD,
 XlsxIbyFileD,
 MongoDBD,
 ListD,
 Migration, Migration2DB,
 open_log
)


def test1():
    """
    Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库
  
    mysql es等均用流式游标读取的方式，避免程序迁移过程中占用过多内存，导致内存爆炸
    将mysql://localhost:3306/test的user表全量迁移到mysql://192.168.0.100:3306/test的user表，若mysql://192.168.0.100:3306/test没有user表，会自动建立数据表。
    """
    open_log()
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                                user='root', passwd='root'),  # 数据来源数据库
        database_to=MySqlD(host='192.168.0.100', port=3306, database='test',
                                user='root', passwd='root'),  # 数据去向数据库
        table_from='user',  # 数据来源数据库表名
        table_to='user'  # 数据去向数据库表名
    )
    t.run()

def test2():
    """
    Migration2DB 依据两个表各自主键合并两个表，生成一个新表
    将./data/userinfo.xlsx和./data/user.xlsx文件，依据user_id列和userid列合并，自动建立./data/user_info_new.xlsx文件。
  
    ./data/userinfo.xlsx:
    user_id sex
    a123    男
    b2333   女
  
    +
  
    ./data/user.xlsx
    userid addr
    a123   china
    c222   地球

    =

    ./data/user_info_new.xlsx
    user_id userid sex  addr
    a123    a123   男   china
    b2333          女
    c222                地球
    """

    t = Migration2DB(
        database_from1=XlsxIbyFileD(path='./data'),  # 数据来源数据库1
        database_from2=XlsxIbyFileD(path='./data'),  # 数据来源数据库2
        table_from1=f'''userinfo''',     # 数据来源数据库表名1
        table_from2=f'''user''',         # 数据来源数据库表名2
        migration_key1=f'''user_id''',   # 数据来源数据库表1主键
        migration_key2=f'''userid''',    # 数据来源数据库表2主键
        database_to=XlsxIbyFileD(path='./data'),    # 数据去向数据库
        table_to='user_info_new',    # 数据去向数据库表名
        # size=10000,
    )
    t.run()


def test3():
    """
    同上test1 Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库
  
    将mysql://localhost:3306/test的user1表全量迁移到es数据库的user1 index，若es没有user1 index，会自动建立index。
    """
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                                   user='root', passwd='root'),
        database_to=ElasticSearchD(hosts='127.0.0.1:9200'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test4():
    """
    同上test1 Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库
  
    将mysql://localhost:3306/test的user1表全量保存到本地csv文件./data/user1。
    """
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='root', passwd='root'),
        database_to=CsvD(path='./data'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test5():
    """
    同上test1 Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库
  
    将本地csv文件./data/user1全量保存到本地csv文件./data/user2。同时修改部分字段
  
    ./data/user1.csv
    user_id sex
    a111    1
  
    ./data/user2.csv
    uuid sex time       tag
    a111 1   1638410972 xs

    """
    import time
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=CsvD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    def self_format(data):
        data['uuid'] = data.pop('user_id')
        data['time'] = int(time.time())
        data['tag'] = 'xs'
        return data
    t.format_data = self_format
    t.run()


def test6():
    """
    同上test1 Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库
  
    在程序内存中构造数据ListD user1全量保存到mysql。同时修改部分字段
  
    ListD('user1').data = {'user1':
        [
            {"user_id":"a111", "sex":1},
            {"user_id":"a112", "sex":2},
        ]
    }

    mysql user表
    user_id sex time       tag
    a111    1   1638410972 xs
    a112    2   1638410973 xs
    """
    import time
    data = [
            {"user_id":"a111", "sex":1},
            {"user_id":"a112", "sex":2},
    ]
    t = Migration(
        database_from=ListD('user1', data=data),
        database_to=MySqlD(host='localhost', port=3306, database='test',
                             user='root', passwd='root'),
        table_from='user1',
        table_to='user'
    )
    def self_format(data):
        data['uuid'] = data.pop('user_id')
        data['time'] = int(time.time())
        data['tag'] = 'xs'
        return data
    t.format_data = self_format
    t.run()


def test7():
    """
    独立使用 CsvD 样例
    Using CsvD independently
  
    如果不想使用Migration 或者Migration2DB，也可以单独使用CsvD、MysqlD等利用同样的函数名函数save_data、get_data快速操作各种地方的数据
    """
    data = CsvD(path='./data').get_data('user')
    file = CsvD(path='./data1')
    first_line = True
    windows = 100
    size = 10
    save_list = []
    for idx, line_dict in enumerate(data):
        if size and size == idx:
            break
        if first_line:
            file.create_index('user1', line_dict, pks='')
            first_line = False
        save_list.append(line_dict)
        if not idx % windows:
            file.save_data('user1', save_list)
            save_list = []
    if save_list:
        file.save_data('user1', save_list)


def test_csvtosql():
    """
    将csv数据文件变成.sql文件，同时改变insert语句生成格式为 ON DUPLICATE KEY UPDATE 
    """

    t = Migration(
        database_from=CsvD(path='/home/user/Desktop'),
        database_to=SqlFileD(path='../data1', compress=True),
        table_from='user',
        table_to='user',
        windows=1000,
        save_data_kwargs={
            "update": 'ON DUPLICATE KEY UPDATE `timeupdate`=(timeupdate), last_active_time=GREATEST(VALUES(last_active_time), last_active_time)'
        }
    )
    t.run()
  

def test_get_ftp_file_until_success():
    """
    从ftp下载文件，不成功会一直自动重试登录客户端再重试断点续传，并且保存已经下载的文件路径，缓存在本地磁盘，做记录，避免重复下载
    """
    from d22d.model.ftpmodel import FtpClientStore, time_stamp
    from d22d.model.diskcachemodel import DiskCacheStore
    from d22d.utils import active_log
    active_log()  # 打开日志
    from d22d.utils import get_realpath_here
    print(get_realpath_here(with_lineno=True))
    ds = DiskCacheStore('disk_cache_ftp_index')  # 创建已传输磁盘记录节约网络开销
    fs = FtpClientStore(
        '123.123.123.123', 21, 'user1', '123123123',
        location='/',  # 连接ftp后打开的工作目录
        tmp_path='local_download_path',  # 本地的临时存放文件工作目录
        use_tls=True, pasv=None, encoding='utf-8',
        # socks_proxy=('192.168.0.216', 38450)
    )

    def download():
        for data in fs.list_data():
            print(data)
            path = data['realpath']
            if size := data.get("size") > 150*1024*1024:    # 文件大于150MB才从FTP下载
                if not ds.check_data(path):
                    fs.get_data(data)
  
            ds.save_data(path, time_stamp())
  
    def upload():
        fs.save_data("1.2.mp4",
                     '/home/user/1.mp4')
    download()


if __name__ == '__main__':
    test5()

```

1. 命令行执行python .py
2. 观察数据迁移时输出的日志，耐心等待程序执行完毕

## 注意事项

* 程序是如何工作的：

以mysql为例，把一个database的一张表作为一个操作的最小单元（后称表格、Table），转移到其他数据库的Table中

* 现在已经支持的数据库类型：

<table>
    <tr>
    <th>数据库类型</th>
    <th>Table来源</th>
    <th>Table名</th>
    </tr>
    <tr> <td>MySQL</td>              <td>tables</td>            <td>tablename</td> </tr>
    <tr> <td>ElasticSearch</td>      <td>indexes</td>           <td>index name</td> </tr>
    <tr> <td>MongoDB</td>            <td>collections</td>       <td>collection name</td> </tr>
    <tr> <td>Excel</td>              <td>file all sheet</td>    <td>filename</td> </tr>
    <tr> <td>ClickHouse</td>              <td>tables</td>    <td>tablename</td> </tr>
    <tr> <td>Oracle</td>              <td>tables</td>    <td>tablename</td> </tr>
    <tr> <td>.json</td>              <td>file</td>              <td>filename</td> </tr>
    <tr> <td>.csv</td>               <td>file</td>              <td>filename</td> </tr>
    <tr> <td>.sql</td>               <td>file</td>              <td>tablename in file</td> </tr>
    <tr> <td>内存中,in mem,ListD</td> <td>dict</td>              <td>dict key</td> </tr>
</table>

* 在入库mongodb时，限于mongodb数据库机制，无法高效自动去重，现在为了追求mongodb入库效率,
  如mongodb有重名collection会先重命名原始collection，格式为 原collection+时间+bak，再新建collection进行入库
* Logstash, pandas是类似的开源项目，但配置繁琐，学习曲线相对陡峭，入库效率不高，无法流式处理数据，无法灵活利用代码处理每一行数据等。
  本项目以极简主义为原则力求用最少的代码、最简单的配置解决用户最迫切的核心需求--数据快速转移

## TODO List

1. Hive
2. HDFS
3. TiDB
4. mysql auto adjust data length
5. mysql auto create index
6. mongodb primary key
7. mongodb duplicated
8. ftp file
9. sftp file
10. excel index by Multi sheet(XlsIbySheetD XlsxIbySheetD)
11. sqlite
12. json dict
13. redis
14. Multi process
15. cron
16. img

# upload pypi

```shell
git config --global http.proxy socks5://127.0.0.1:8389
git config https.proxy socks5://127.0.0.1:8389
python setup.py sdist bdist_wheel upload
twine upload dist/*
```
