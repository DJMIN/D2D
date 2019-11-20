# D2D
Database To Database, The fastest migration tool for all database by scheduled tasks with
2 lines of code.    
(.csv .sql .json .xls .xlsx mysql ElasticSearch excel mongodb sqlite redis...)    
2行代码即可使用最高效的方式最快的速度在各种数据库中迁移数据，并且可以实行计划任务，定时迁移增量数据

Can automatically build a table based on data    
可以根据数据自动建库建表，而无需操心数据格式和类型

Has detailed logging    
拥有详细的日志记录


## Install 如何安装

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
    
## RUN 如何运行

1. 配置好run.py文件里Migration类的参数（现已有15个demo函数在文件中，可以参照修改）
1. 命令行执行python run.py
1. 观察输出日志，耐心等待程序执行完毕

###### Migration类的参数定义

database_from： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等     
database_to： 数据库类 如： ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD等    
data_from： index名或table名或文件名（如不带则会遍历所有数据库表进行迁移到指定数据库同名表）    
data_to： index名或table名或文件名  
    

## 注意事项

在入库mongodb时，限于mongodb数据库机制，无法高效自动去重，现在为了追求mongodb入库效率,
如mongodb有重名collection会先重命名原始collection，格式为 原collection+时间+bak，再新建collection进行入库

## TODO List 

1. mysql auto adjust data length
1. mysql auto create index
1. mongodb primary key
1. mongodb duplicated
1. ftp file
1. sftp file
1. .sql file
1. excel index by Multi sheet(XlsIbySheetD XlsxIbySheetD)
1. sqlite
1. json dict
1. redis
1. Multi process
1. cron
1. img