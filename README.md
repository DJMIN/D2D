# D2D
Database to database, The fastest migration tool for all database by scheduled tasks with
2 lines of code.    
(.csv .sql .json .xls .xlsx mysql ElasticSearch excel mongodb sqlite redis...) 
2行代码即可使用最高效的方式最快的速度在各种数据库中迁移数据，并且可以实行计划任务，定时迁移增量数据

Can automatically build a table based on data
可以根据数据自动建库建表，而无需操心数据格式和类型

Has detailed logging
拥有详细的日志记录


## Install


python3.7以上

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

    git clone https://github.com/DJMIN/D2D.git
    cd D2D
    pip3.7 install -r requirements.txt

## RUN

###Migration类的参数定义

× database_from： 数据库类    
× database_to： 数据库类    
× data_from： index名或table名或文件名，如不带则会遍历所有数据库表进行迁移到指定数据库同名表    
× data_to： index名或table名或文件名，如不带则会遍历所有数据库表进行迁移到指定数据库同名表    
    
    python run.py

---

如果是内网用户无法连接互联网下载python环境，无法pip install，这里贴心的为你准备了python虚拟环境包，使用方法：

    cd D2D
    vim venv-ubuntu-x64/bin/activate
    
修改venv-ubuntu-x64/bin/activate文件的这一行(应该是第40行)为你项目文件的目录下的D2D/venv-ubuntu-x64路径，这里必须是绝对路经

    VIRTUAL_ENV="/home/user/PycharmProjects/D2D/venv-ubuntu-x64"
    
    
然后回到终端窗口

    source venv-ubuntu-x64/bin/activate
    
最后就可以开开心心 

    python run.py
    
    
## TODO List

1. mysql auto adjust data length
1. mysql auto create index
1. mongodb
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