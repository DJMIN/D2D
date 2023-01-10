#!/usr/bin/env python
# coding=utf-8
from d22d.version import __VERSION__

try:
    from setuptools import setup, find_packages
except:
    from distutils.core import setup

    find_packages = lambda x: []

# def read(fname):
#     return codecs.open(os.path.join(os.path.dirname(__file__), fname)).read()
#
#
# long_des = read("README.md")

with open('README.md', encoding='utf-8') as f:
    long_text = f.read()

with open('requirements.txt', encoding='utf-8') as f:
    install_requires = [
        line for line in f.read().strip().splitlines()
        if not line.startswith('#')]

setup(
    name='d22d',
    version=__VERSION__,
    description=(
        'Migrating form DataBase to DataBase by 2 lines code'
    ),
    long_description=long_text,
    long_description_content_type="text/markdown",
    author='readerror',
    author_email='readerror@sina.com',
    maintainer='readerror',
    maintainer_email='readerror@sina.com',
    license='GPL License',
    packages=find_packages(),
    platforms=["all"],
    zip_safe=True,
    package_data={'': ['*']},
    url='https://github.com/DJMIN/D2D',
    python_requires='>=3.6',
    install_requires=install_requires,
    extras_require={
        'OracleD': [
            "cx_Oracle==7.2.3",
            # 'oracle数据库：11.2.0.2.0 - 64bit',
            # 'instantclient：11.2.0.4.0 - 64bit'
        ],
        'http': ["http-parser"],
        'rar': ["unrar-cffi"],
    }
)
