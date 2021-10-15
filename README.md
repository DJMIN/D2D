# d22d
# D22[twaɪs]D 
Migrating form DateBase to DateBase by 2 lines code, The fastest migration tool for all database by scheduled tasks with
2 lines of code.    
(.csv .sql .json .xls .xlsx mysql ElasticSearch excel mongodb sqlite redis...)    
2行代码即可使用最高效的方式最快的速度在各种数据库中迁移数据，并且可以实行计划任务，定时迁移增量数据

Can automatically build a table based on data    
可以根据数据自动建库建表，而无需操心数据格式和类型

Has detailed logging    
拥有详细的日志记录


## Install 如何安装
* 以下说明完全只针对安装源码调试的用户， 
普通用户sudo pip install d22d 已可使用，可跳过本节安装说明直至["RUN 如何使用"](#trun)
#### 如果是互联网用户

##### 安装python3.7以上如有可跳过

###### python3 centos

```shell script
yum install -y wget tar libffi-devel zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gcc make 
wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz
tar -zxvf Python-3.7.0.tgz
cd Python-3.7.0
./configure
make&&make install
```

###### python3 ubuntu

```shell script
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

#### 如果是内网用户无法连接互联网下载python环境，无法pip install安装python依赖包，这里贴心的为你准备了python虚拟环境包，使用方法：

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
    
##  <a name="trun">RUN 如何使用</a>

1. need python3
1. create your .py file 
```python
import d22d
from d22d import (
ElasticSearchD, MySqlD, CsvD, SqlFileD, JsonListD,
 XlsIbyFileD, XlsxIbyFileD, MongoDBD, 
 Migration, Migration2DB, open_log
)

def test1():
    open_log()
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                                user='root', passwd='root'),
        database_to=MySqlD(host='192.168.0.100', port=3306, database='test',
                                user='root', passwd='root'),
        table_from='user',
        table_to='user'
    )
    t.run()

def test2():
    t = Migration2DB(
        database_from1=XlsxIbyFileD(path='./data'),
        database_from2=XlsxIbyFileD(path='./data'),
        table_from1=f'''userinfo''',
        table_from2=f'''user''',
        migration_key1=f'''user_id''',
        migration_key2=f'''userid''',
        database_to=XlsxIbyFileD(path='./data'),
        table_to='user_info_new',
        # size=10000,
    )
    t.run()


def test3():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                                   user='root', passwd='root'),
        database_to=ElasticSearchD(hosts='127.0.0.1:9200'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test4():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='root', passwd='root'),
        database_to=CsvD(path='./data'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test5():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=CsvD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    t.run()


def test6():
    """
    独立使用 CsvD 样例
    Using CsvD independently

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
    

if __name__ == '__main__':
    test5()

```

1. 命令行执行python .py
1. 观察数据迁移时输出的日志，耐心等待程序执行完毕


###### Migration类的参数定义

database_from： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等     
database_to： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等    
table_from： index名或table名或文件名（如不带则会遍历所有数据库表进行迁移到指定数据库同名表）    
table_to： index名或table名或文件名  
    

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
    <tr> <td>MySQL</td>            <td>tables</td>            <td>tablename</td> </tr>
    <tr> <td>ElasticSearch</td>    <td>indexes</td>           <td>index name</td> </tr>
    <tr> <td>MongoDB</td>          <td>collections</td>       <td>collection name</td> </tr>
    <tr> <td>Excel</td>            <td>file all sheet</td>              <td>filename</td> </tr>
    <tr> <td>.json</td>            <td>file</td>              <td>filename</td> </tr>
    <tr> <td>.csv</td>             <td>file</td>              <td>filename</td> </tr>
</table>

* 在入库mongodb时，限于mongodb数据库机制，无法高效自动去重，现在为了追求mongodb入库效率,
如mongodb有重名collection会先重命名原始collection，格式为 原collection+时间+bak，再新建collection进行入库

* Logstash是类似的开源项目，但配置繁琐，学习曲线相对陡峭，入库效率不高。
本项目以极简主义为原则力求用最少的代码、最简单的配置解决用户最迫切的核心需求--数据快速转移
## TODO List 

1. mysql auto adjust data length
1. mysql auto create index
1. mongodb primary key
1. mongodb duplicated
1. ftp file
1. sftp file
1. excel index by Multi sheet(XlsIbySheetD XlsxIbySheetD)
1. sqlite
1. json dict
1. redis
1. Multi process
1. cron
1. img

# upload pypi

```shell
python setup.py sdist bdist_wheel upload
twine upload dist/*
```