#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages
from version import __VERSION__
setup(
    name='d22d',
    version=__VERSION__,
    description=(
        'Migrating form DataBase to DataBase by 2 lines code'
    ),
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author='readerror',
    author_email='readerror@sina.com',
    maintainer='readerror',
    maintainer_email='readerror@sina.com',
    license='GPL License',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/DJMIN/D2D',
    python_requires='>=3.6',
    install_requires=[
        "python-logstash",
        "gunicorn",
        "kafka-python",
        "chardet",
        "baostock",
        "flask_socketio",
        "async-timeout",
        "thrift",
        "pyquery",
        "dnspython",
        "Flask_Migrate",
        "coverage",
        "bs4",
        "itsdangerous",
        "pysocks",
        "Werkzeug",
        "pathlib2",
        "nose",
        "zipp",
        "pymongo",
        "psycopg2-binary",
        "Flask_login",
        "logstash_formatter",
        "PyMySQL",
        "urllib3",
        "Flask_Cache",
        "python-logstash-async",
        "gevent",
        "xlsxwriter",
        "mongoengine",
        "phone",
        "Flask_Cors",
        "click",
        "Flask",
        "rednose",
        "eventlet",
        "func_timeout",
        "simplejson",
        "PyHive",
        "cryptg",
        "msgpack",
        "concurrent-log-handler",
        "Flask-Migrate",
        "watchdog",
        "docker",
        "requests[socks]",
        "openpyxl",
        "pymysql",
        "flask",
        "lxml",
        "pandas",
        "python-dateutil",
        "pytest",
        "numpy",
        "SQLAlchemy",
        "Flask_SQLAlchemy",
        "Flask_Script",
        "rarfile",
        "bcrypt",
        "greenlet",
        "xlrd",
        "PySocks",
        "demjson",
        "tushare",
        "psutil",
        "arrow",
        "tornado",
        "flask_mongoengine",
        "celery",
        "flask_sqlalchemy",
        "setuptools",
        "pycryptodome",
        "wrapt",
        "twine",
        "sqlparse",
        "six",
        "requests>=2.22.0",
        "elasticsearch_dsl>=6.1",
        "PyMySQL>=0.9.3",
        "mysql-connector-python>=8.0.6",
        "redis>=3.2.1",
        "DBUtils>=2.0",
        "elasticsearch>=6.1.1",
        "diskcache",
        "tomorrow3",
        "clickhouse_driver",
        "zip-files",
        "unrar-cffi",
        "paramiko",
        "pyftpdlib",
        "pysnooper",
    ],
)