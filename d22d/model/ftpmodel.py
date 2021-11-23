# -*- coding: utf-8 -*-
import datetime
import os
import logging
import time
from os.path import isfile
import shutil
import sys
import ftplib
from ftplib import FTP_TLS
from threading import Lock
import socket
import io
import socks  # socksipy (https://github.com/mikedougherty/SocksiPy)
import typing
import wrapt
from tempfile import TemporaryFile

from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.authorizers import DummyAuthorizer

from d22d.model import midhardware
from d22d.utils import log_info
from d22d.utils.utils import path_leaf, time_stamp
from d22d.utils.ziputils import makedirs

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class FTPSocks(ftplib.FTP):
    def __init__(self, host='', user='', passwd='', acct='',
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None,
                 proxyconfig=None):
        """Like ftplib.FTP constructor, but with an added `proxyconfig` kwarg

        `proxyconfig` should be a dictionary that may contain the following
        keys:

        proxytype - The type of the proxy to be used. Three types
                are supported: PROXY_TYPE_SOCKS4 (including socks4a),
                PROXY_TYPE_SOCKS5 and PROXY_TYPE_HTTP
        addr -      The address of the server (IP or DNS).
        port -      The port of the server. Defaults to 1080 for SOCKS
                servers and 8080 for HTTP proxy servers.
        rdns -      Should DNS queries be preformed on the remote side
                (rather than the local side). The default is True.
                Note: This has no effect with SOCKS4 servers.
        username -  Username to authenticate with to the server.
                The default is no authentication.
        password -  Password to authenticate with to the server.
                Only relevant when username is also provided.
        """
        self.proxyconfig = proxyconfig or {}
        ftplib.FTP.__init__(self, host, user, passwd, acct, timeout, source_address)

    def __str__(self):
        return f"ftp://{self.host}:{self.port} with proxy={self.proxyconfig}"

    def connect(self, host='', port=0, timeout=-999, source_address=None):
        """Connect to host.  Arguments are:
         - host: hostname to connect to (string, default previous host)
         - port: port to connect to (integer, default previous port)
         - timeout: the timeout to set against the ftp socket(s)
         - source_address: a 2-tuple (host, port) for the socket to bind
           to as its source address before connecting.
        """
        if host != '':
            self.host = host
        if port > 0:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        if source_address is not None:
            self.source_address = source_address
        logger.info(f'正在连接FTP{self}')
        sys.audit("ftplib.connect", self, self.host, self.port)
        self.sock = self.create_connection(self.host, self.port)
        self.af = self.sock.family
        self.file = self.sock.makefile('r', encoding=self.encoding)
        self.welcome = self.getresp()
        return self.welcome

    def create_connection(self, host=None, port=None):
        host, port = host or self.host, port or self.port
        if self.proxyconfig:
            phost, pport = self.proxyconfig['addr'], self.proxyconfig['port']
            err = None
            for res in socket.getaddrinfo(phost, pport, 0, socket.SOCK_STREAM):
                af, socktype, proto, canonname, sa = res
                sock = None
                try:
                    sock = socks.socksocket(af, socktype, proto)
                    sock.setproxy(**self.proxyconfig)

                    if self.timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                        sock.settimeout(self.timeout)
                    sock.connect((host, port))
                    return sock
                except socket.error as _:
                    err = _
                    if sock is not None:
                        sock.close()
            if err is not None:
                raise err
            else:
                raise socket.error("getaddrinfo returns an empty list")
        else:
            sock = socket.create_connection((host, port), self.timeout)
        return sock

    def ntransfercmd(self, cmd, rest=None):
        """Initiate a transfer over the data connection.

        If the transfer is active, send a port command and the
        transfer command, and accept the connection.  If the server is
        passive, send a pasv command, connect to it, and start the
        transfer command.  Either way, return the socket for the
        connection and the expected size of the transfer.  The
        expected size may be None if it could not be determined.

        Optional `rest' argument can be a string that is sent as the
        argument to a REST command.  This is essentially a server
        marker used to tell the server to skip over any data up to the
        given marker.
        """
        size = None
        if self.passiveserver:
            host, port = self.makepasv()
            conn = self.create_connection(host, port)
            try:
                if rest is not None:
                    self.sendcmd("REST %s" % rest)
                resp = self.sendcmd(cmd)
                # Some servers apparently send a 200 reply to
                # a LIST or STOR command, before the 150 reply
                # (and way before the 226 reply). This seems to
                # be in violation of the protocol (which only allows
                # 1xx or error messages for LIST), so we just discard
                # this response.
                if resp[0] == '2':
                    resp = self.getresp()
                if resp[0] != '1':
                    raise ftplib.error_reply(resp)
            except:
                conn.close()
                raise
        else:
            with self.makeport() as sock:
                if rest is not None:
                    self.sendcmd("REST %s" % rest)
                resp = self.sendcmd(cmd)
                # See above.
                if resp[0] == '2':
                    resp = self.getresp()
                if resp[0] != '1':
                    raise ftplib.error_reply(resp)
                conn, sockaddr = sock.accept()
                if self.timeout is not ftplib._GLOBAL_DEFAULT_TIMEOUT:
                    conn.settimeout(self.timeout)
        if resp[:3] == '150':
            # this is conditional in case we received a 125
            size = ftplib.parse150(resp)
        return conn, size

    # def ntransfercmd(self, cmd, rest=None):
    #     size = None
    #     if self.passiveserver:
    #         host, port = self.makepasv()
    #         conn = self.create_connection(host, port)
    #         try:
    #             if rest is not None:
    #                 self.sendcmd("REST %s" % rest)
    #             resp = self.sendcmd(cmd)
    #             # Some servers apparently send a 200 reply to
    #             # a LIST or STOR command, before the 150 reply
    #             # (and way before the 226 reply). This seems to
    #             # be in violation of the protocol (which only allows
    #             # 1xx or error messages for LIST), so we just discard
    #             # this response.
    #             if resp[0] == '2':
    #                 resp = self.getresp()
    #             if resp[0] != '1':
    #                 raise ftplib.error_reply(resp)
    #         except:
    #             conn.close()
    #             raise
    #     else:
    #         raise Exception("Active transfers not supported")
    #     if resp[:3] == '150':
    #         # this is conditional in case we received a 125
    #         size = ftplib.parse150(resp)
    #     return conn, size


def set_file_log(log):
    # create a file handler
    handler = logging.FileHandler('ftp_log.log')
    handler.setLevel(logging.DEBUG)

    # create a logging format
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s]: %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    log.addHandler(handler)


def to_simple_chinese(string):
    # otherLanguage 变成中文
    item = string.encode('iso-8859-1').decode('gbk')
    return item


def to_latin_language(string):
    # 简体中文变成 latin
    item = string.encode('gbk').decode('iso-8859-1')
    return item


ERR_STR_SETTING = {
    "主动模式只适用于客户端有公网IP或者服务器与客户端在同一个网段下": [
        # "ftplib.error_perm: 500 I won't open a connection to 192.168.18.111 (only to 222.111.333.111)"
        " I won't open a connection to ",
        "(only to",
    ],
    "File exists": [
        "File exists"
    ],
    "Not a directory": [
        "Not a directory"
    ],
    "No such file or directory": [
        "No such file or directory"
    ],
    "Can't check for file existence": [
        # ftplib.error_perm: 550 Can't check for file existence
        "Can't check for file existence"
    ],
    "Can't change directory to": [
        # Can't change directory to 123f: No such file or directory
        "Can't change directory to"
    ],
    "(measured here),": [
        # 出现于被动模式断点续传从ftp下载文件的时候，
        # 第一次以为是客户端retrbinary函数本来是执行二进制命令，带有文件大小参数以后，无法执行带有中文名的文件路径编码导致
        # 第二次以为是客户端发送命令之后，没返回要的结果，返回了上次还未执行完毕的命令返回的结果冲突导致（下载文件还没完成返回了）
        # ftplib.error_reply: 150 0.926 seconds (measured here), 0.54 Mbytes per second
        "(measured here),"
    ],
    "Unknown command": [
        # 出现于被动模式断点续传从ftp下载文件的时候， 服务端不支持断点续传
        "Unknown command"
    ],
}


def get_err_str_setting(err_key, ex):
    return any(err_str in str(ex) for err_str in ERR_STR_SETTING[err_key])


def with_ftp_lock():
    """
    请求self.ftp,自带线程锁
    """

    @wrapt.decorator
    def wrapper(func, instance, args, kwargs):
        res = None
        while True:
            lock = instance.ftp_lock
            if lock.locked():
                raise SystemError(f'FTP客户端函数[{func.__name__}]执行错误 ftp locked')
            lock.acquire()
            try:
                res = func(*args, **kwargs)
                break
            except ftplib.error_perm as inst:
                if get_err_str_setting('主动模式只适用于客户端有公网IP或者服务器与客户端在同一个网段下', inst):
                    logger.error("主动模式只适用于客户端有公网IP或者服务器与客户端在同一个网段下")
                    instance.try_all_connect_to(False)
                logger.error(f"FTP客户端函数[{func.__name__}]执行错误：[{inst}] {args} {kwargs}")
                raise inst
            except NETWORK_ERR as inst:
                logger.error(f"FTP客户端函数[{func.__name__}]执行错误：网络中断导致{instance}服务端中断,错误：[{inst}]，5秒后重试...")
                time.sleep(5)
                instance.connect_until_success()
            except Exception as inst:
                raise inst
            finally:
                lock.release()
        return res

    return wrapper


NETWORK_ERR = (
    socket.timeout,  # 直接断了
    EOFError,  # 用代理断了
    socks.ProxyConnectionError,  # 用代理的时候被拔网线
    OSError  # 被拔网线
)


class FtpController:
    # /!\ Although the comments and variable names say 'file_name'/'file_anything' it inculdes folders also
    # Some functions in this class has no exception handling, it has to be done outside

    def __init__(
            self, host, port=21, username=' ', password=' ', use_tls=False, pasv=True, encoding='utf-8',
            proxy_config=None
    ):
        # List to store file search and search keywords
        self.search_file_list = []
        self.detailed_search_file_list = []
        self.keyword_list = []

        # Variable to hold the max no character name in file list (used for padding in GUIs)
        self.max_len = 0

        self.max_len_name = ''

        # Variable to tell weather hidden files are enabled
        self.hidden_files = False

        # Variable to store the platform the server is running on
        self.server_platform = 'Linux'
        self.lock = Lock()
        self.ftp_lock = Lock()
        self.ftp = None
        self.remote_directory_path = '/'
        self.work_dir_now = '/'
        self.filesMoved = 0
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        self.up_file_size_start = 0
        self.last_log = 0

        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.use_tls = use_tls
        self.pasv = pasv
        self.encoding = encoding
        self.encoding_test = None
        self.proxyconfig = proxy_config

    def __str__(self):
        return f"ftp://{self.host}:{self.port}{self.work_dir_now}"

    def connect_until_success(self, retry=0):
        cnt_retry = 0
        while True:
            cnt_retry += 1
            try:
                log_info(f"正在尝试第[{cnt_retry}/{retry}]次登录[{self}]。。。")
                # 先无tls，在试试有tls
                self.ftp = None
                if not self.use_tls:
                    self.ftp = FTPSocks(
                        timeout=25, proxyconfig=self.proxyconfig)
                    self.ftp.connect(self.host, self.port)
                else:
                    self.ftp = FTP_TLS(timeout=25)
                    self.ftp.connect(self.host, self.port)
                self.ftp.login(self.username, self.password)

                if self.use_tls:
                    self.ftp.prot_p()
                self.ftp.cwd(self.work_dir_now)
                if self.pasv is not None:
                    self.ftp.set_pasv(self.pasv)
                self.ftp.encoding = self.encoding
                logger.info(f"Connected. now pwd: {self.work_dir_now}")
                return
            except Exception as inst:
                if retry and cnt_retry > retry:
                    raise inst
                print_func = logger.error
                if not isinstance(inst, NETWORK_ERR):
                    print_func = logger.exception
                print_func(f"第[{cnt_retry}/{retry}]次登录[{self}]失败 [{type(inst)}] {inst}, 5秒后重试。。。")
                time.sleep(5)

    def try_all_connect_to(self, no_pasv=True):
        pasv_d = {
            True: "被动模式",
            False: "主动模式",
            None: "默认模式"  # 如果 val 为 true，则打开“被动”模式，否则禁用被动模式。默认下被动模式是打开的。主动模式只适用于客户端有公网IP或者服务器在同一个网段下

        }
        for tls, pasv in [
                             (True, None),
                             (False, None),
                             (True, True),
                             (False, True),
                         ] + ([
            (True, False),
            (False, False),
        ] if no_pasv else []):
            if self.try_connect_to(tls, pasv):
                logger.info(f'成功! 登录尝试FTP：{"使用" if tls else "无"}tls {pasv_d[pasv]}')
                return tls, pasv
            else:
                logger.info(f'尝试登录FTP失败：{"使用" if tls else "无"}tls {pasv_d[pasv]}')
        raise SystemError(f'尝试登录FTP全部失败{self}')

    def try_connect_to(self, use_tls, pasv, encoding="utf-8"):
        try:
            # 先无tls，在试试有tls
            self.ftp = None
            if not use_tls:
                self.ftp = FTPSocks(
                    timeout=25, proxyconfig=self.proxyconfig)
                self.ftp.connect(self.host, self.port)
            else:
                self.use_tls = True
                self.ftp = FTP_TLS(timeout=3)
                self.ftp.connect(self.host, self.port)
            self.ftp.login(self.username, self.password)

            if use_tls:
                self.ftp.prot_p()
            self.ftp.cwd(self.remote_directory_path)
            if pasv is not None:
                self.ftp.set_pasv(pasv)
            self.ftp.encoding = encoding
            logger.info(f"Connected. now pwd: {self.remote_directory_path}")
            self.use_tls = use_tls
            self.pasv = pasv
            self.encoding = encoding
            return True
        except NETWORK_ERR as inst:
            logger.error(f"重试连接{self}服务端中断,错误：[{inst}]")
        except Exception as inst:
            logger.exception(f"Couldn't connect server: {type(inst)}")
            return False

    def check_encoding(self, encoding="utf-8"):
        logger.info(f"正在测试到服务端encoding值")
        old_encoding = encoding
        self.ftp.encoding = encoding
        self.encoding_test = None
        test_string = '测试用Test，空文件夹，看到请顺手删除，请勿往里面存数据，Please_delete_handy!'
        # TODO 新建中文名来测试文件夹名是否能取到
        self.make_dir_optimistic(test_string)
        for file in self.get_detailed_file_list():
            if test_string in file:
                self.encoding = encoding
                self.encoding_test = encoding
                logger.info(f"正确测试到服务端encoding值为：{encoding}")
                break
        if not self.encoding_test:
            logger.info(f"测试到服务端encoding值不为：{encoding}，请更换")
            self.ftp.encoding = old_encoding
            self.encoding_test = None
        self.delete_dir_fast(test_string)

    def disconnect(self):
        self.ftp.quit()  # Close FTP connection
        self.ftp = None

    def toggle_hidden_files(self):
        self.hidden_files = not self.hidden_files

    @with_ftp_lock()
    def get_detailed_file_list(self, ignore_hidden_files_flag=False):
        files = []

        def dir_callback(line):
            if self.server_platform != 'Linux':
                files.append(line)
                return
            if (self.hidden_files is True or line.split()[8][0] != '.') or ignore_hidden_files_flag is True:
                files.append(line)

        self.ftp.dir(dir_callback)
        return files

    def get_file_list(self, detailed_file_list):
        self.max_len = 0
        self.max_len_name = ''
        file_list = []
        for x in detailed_file_list:
            # Remove details and append only the file name
            if self.server_platform == 'Linux':
                name = self.get_properties(x)[0]
            else:
                name = x
            file_list.append(name)
            if len(name) > self.max_len:
                self.max_len = len(name)
                self.max_len_name = name
        return file_list

    def get_detailed_search_file_list(self):
        return self.detailed_search_file_list

    def get_search_file_list(self):
        self.max_len = 0
        self.max_len_name = ''
        for name in self.search_file_list:
            if len(name) > self.max_len:
                self.max_len = len(name)
                self.max_len_name = name
        return self.search_file_list

    def chmod(self, filename, permissions):
        self.ftp.sendcmd('SITE CHMOD ' + str(permissions) + ' ' + filename)

    def is_there(self, path):
        try:
            self.ftp.sendcmd('MLST ' + path)
            return True
        except:
            return False

    def rename_dir(self, rename_from, rename_to):
        self.ftp.sendcmd('RNFR ' + rename_from)
        self.ftp.sendcmd('RNTO ' + rename_to)

    def move_dir(self, rename_from, rename_to, status_command=log_info, replace_command=log_info):
        if self.is_there(rename_to) is True:
            if replace_command(rename_from, 'File/Folder exists in destination folder') is True:
                self.delete_dir(rename_to, status_command)
            else:
                return
        try:
            self.ftp.sendcmd('RNFR ' + rename_from)
            self.ftp.sendcmd('RNTO ' + rename_to)
            status_command(rename_from, 'Moved')
        except:
            status_command(rename_from, 'Failed to move')

    def copy_file(self, file_dir, copy_from, file_size, status_command=log_info, replace_command=log_info):
        # Change to script's directory
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        if not os.path.exists('copy_temps'):
            os.makedirs('copy_temps')
        os.chdir('copy_temps')
        # Save the current path so that we can copy later
        dir_path_to_copy = self.ftp.pwd()
        # Change to the file's path and download it
        self.ftp.cwd(file_dir)
        self.download_file(copy_from, file_size, status_command, replace_command)
        # Change back to the saved path and upload it
        self.ftp.cwd(dir_path_to_copy)
        self.upload_file(copy_from, file_size, status_command, replace_command)
        # Delete the downloaded file
        os.remove(copy_from)
        status_command(copy_from, 'Deleted local file')

    def copy_dir(self, file_dir, copy_from, status_command=log_info, replace_command=log_info):
        # Change to script's directory
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        if not os.path.exists('copy_temps'):
            os.makedirs('copy_temps')
        os.chdir('copy_temps')
        # Save the current path so that we can copy later
        dir_path_to_copy = self.ftp.pwd()
        # Change to the file's path and download it
        self.ftp.cwd(file_dir)
        self.download_dir(copy_from, status_command, replace_command)
        # Change back to the saved path and upload it
        self.ftp.cwd(dir_path_to_copy)
        self.upload_dir(copy_from, status_command, replace_command)
        # Delete the downloaded folder
        shutil.rmtree(copy_from)
        status_command(copy_from, 'Deleting local directory')

    def delete_file(self, file_name, status_command=log_info):
        try:
            self.ftp.sendcmd('DELE ' + file_name)
            status_command(file_name, 'Deleted')
        except:
            status_command(file_name, 'Failed to delete')

    def delete_dir(self, dir_name, status_command=log_info):
        # Go into the directory
        self.ftp.cwd(dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list(True)
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If directory
            if self.is_dir(file_details):
                self.delete_dir(file_name, status_command)
            # If file
            else:
                self.delete_file(file_name, status_command)
        # Go back to parent directory and delete it
        try:
            self.ftp.cwd('..')
            status_command(dir_name, 'Deleting directory')
            self.ftp.sendcmd('RMD ' + dir_name)
        except:
            status_command(dir_name, 'Failed to delete directory')
            return

    @with_ftp_lock()
    def delete_dir_fast(self, dir_name, status_command=log_info):
        status_command(dir_name, '正在删除文件夹，Deleting directory')
        self.ftp.sendcmd('RMD ' + dir_name)

    def upload_file(self, file_name, file_size, status_command=log_info, replace_command=log_info):
        def update_progress(data):
            self.bytes_uploaded += int(sys.getsizeof(data))
            status_command(file_name, str(min(round((self.bytes_uploaded / file_size) * 100, 8), 100)) + '%')

        # Variable to keep trak of number of bytes uploaded
        self.bytes_uploaded = 0
        # Check if the file is already present in ftp server
        if self.is_there(file_name):
            if replace_command(file_name, 'File exists in destination folder') is False:
                return
        # Try to open file, if fails return
        try:
            file_to_up = open(file_name, 'rb')
        except:
            status_command(file_name, 'Failed to open file')
            return
        # Try to upload file
        try:
            status_command(file_name, 'Uploading')
            self.ftp.storbinary('STOR ' + file_name, file_to_up, 8192, update_progress)
            status_command(None, 'newline')
        except:
            status_command(file_name, 'Upload failed')
            return
        # Close file
        file_to_up.close()

    def upload_file_to_some_where(
            self, local_path, remote_folder, remote_filename='', status_command=log_info, check_ftp_file_same=True):
        # TODO BytesIO类型local_path
        if not os.path.exists(local_path):
            raise SystemError(f'本地路径不存在：{local_path.__repr__()}')
        if not remote_folder:
            raise SystemError(f'远程路径错误：{remote_folder.__repr__()} {remote_filename}')
        if not remote_filename:
            remote_folder, remote_filename = os.path.split(remote_folder)

        if not remote_folder.startswith('/'):
            remote_folder = os.path.realpath(os.path.join(self.work_dir_now, remote_folder))
        remote_path = os.path.join(remote_folder, remote_filename)
        old_path = self.work_dir_now
        if remote_folder:
            try:
                self.cwd_recode_path(remote_folder)
            except ftplib.error_perm as ex:
                if get_err_str_setting("Can't change directory to", ex):
                    self.make_dir_optimistic(remote_folder)
        self._upload_file_to_some_where(
            local_path, remote_path, remote_filename, status_command, check_ftp_file_same)
        self.work_dir_now = old_path
        self.cwd_recode_path(old_path)

    @with_ftp_lock()
    def _upload_file_to_some_where(
            self, local_path, remote_path, remote_filename, status_command=log_info, check_ftp_file_same=True):
        file_size = os.stat(local_path).st_size
        try:
            if check_ftp_file_same:
                status_command('正在检查远程已经存在的文件路径上传的文件大小')
                self.up_file_size_start = self.get_size(remote_path) or 0

                _all_size = f'{file_size / 1024 / 1024:.3f}MB'
                _up_size = f'{self.up_file_size_start / 1024 / 1024:.3f}MB'
                status_command(f"服务端已存在同名文件的文件大小:{_all_size},本地{_up_size}")

                def check_file(data):
                    f_data = file_need_up.read(len(data))
                    status_command(
                        f"正在检查远程ftp服务器已经存在的文件路径上传的文件和本地文件一致性：\n",
                        f"本地：{f_data[:70]}\n远程：{data[:70]}\n本地{len(f_data)}=?远程{len(data)}: {f_data == data}")
                    if f_data == data:
                        self.is_same_file = True
                        status_command(f"文件开头800KB一致，准备开始断点续传，已经上传的文件大小:{self.up_file_size_start / 1024 / 1024:.3f}MB", )
                    else:
                        self.is_same_file = False
                        raise SystemError(f'远程文件路径["{self}{remote_path}:1"]已存在，而且本地文件开头800KB与服务器文件不一致，请检查')
                    raise StopIteration('只是检查文件开头一致性，不需要全部下载')

                with open(local_path, 'rb') as file_need_up:
                    status_command(f'正在下载远程服务器文件[{file_size / 1024 / 1024:.3f}MB]的开头800KB进行文件比对，路径：{remote_path}')
                    try:
                        self.ftp.retrbinary('RETR ' + remote_path, check_file, blocksize=1024 * 8 * 128)  # 检查800KB
                    except StopIteration:
                        try:
                            status_command(self.ftp.getmultiline())
                            self.ftp.abort()
                        except NETWORK_ERR:
                            self.ftp.close()
                            self.connect_until_success()

            else:
                self.up_file_size_start = 0
        except ftplib.error_perm as ex:
            if get_err_str_setting("Can't check for file existence", ex):
                self.up_file_size_start = 0
            else:
                raise
        start_time = time.time()
        self.bytes_uploaded = 0
        self.last_log = time.time()

        def upload_file(data):
            self.bytes_uploaded += int(sys.getsizeof(data))
            all_size = f'{file_size / 1024 / 1024:.3f}MB'
            up_size = f'{(self.up_file_size_start + self.bytes_uploaded) / 1024 / 1024:.3f}MB'
            per_second_size = f'{self.bytes_uploaded / 1024 / 1024 / (time.time() - start_time):.3f}MB/s'
            if self.last_log + 2 < (time_now := time.time()):
                self.last_log = time_now
                status_command(remote_path, str(min(
                    round(((self.up_file_size_start + self.bytes_uploaded) / file_size) * 100, 3),
                    100)) + f'% [{up_size}/ {all_size}] {per_second_size}')

        if self.up_file_size_start == file_size:
            return
        elif self.up_file_size_start < file_size:
            with open(local_path, 'rb') as local_file:
                local_file.seek(self.up_file_size_start)
                self.ftp.storbinary(
                    'STOR ' + remote_filename, local_file,
                    blocksize=8192, callback=upload_file, rest=self.up_file_size_start)
        else:
            raise SystemError(f'本地文件大小 小于 远程文件大小：{local_path} --> {self}:{remote_path}')

    @staticmethod
    def get_split_dir_path_list(path):
        res = [(path, '')]
        while True:
            root_path, final_dir = os.path.split(path)
            res.append((root_path, final_dir))
            path = root_path
            if root_path == "" or root_path == '/' or root_path.endswith(':\\'):
                break
        return res

    @with_ftp_lock()
    def make_dir_optimistic(self, path_dir):
        """
        乐观创建文件夹，假定所有父文件都已经被创建，此时效率高，能直接越级创建深层级文件夹。
        :return:
        """

        path_dir_l = self.get_split_dir_path_list(path_dir)
        need_make = []
        logger.info(f'乐观创建文件夹: "{os.path.join(self.work_dir_now, path_dir)}"')
        for root, folder in path_dir_l:
            try:
                if root:
                    self.ftp.mkd(root)
            except (ftplib.error_perm,) as inst:
                # print(root, str(ex))
                if get_err_str_setting('No such file or directory', inst):
                    need_make.insert(0, [root, folder])
                elif get_err_str_setting("File exists", inst):
                    break
                elif get_err_str_setting("Not a directory", inst):
                    raise SystemError(f'创建{self}文件夹时出现同名文件：{root}') from None
                else:
                    raise SystemError(f'创建{self}文件夹时出现问题：{root} {inst}')

        for root, folder in need_make:
            try:
                self.ftp.mkd(root)
            except (ftplib.error_perm,) as ex:
                # print(root, str(ex))
                raise SystemError(f'创建{self}文件夹时出现问题：{root} {ex}')

    def make_dir_pessimistic(self, path_dir):
        """
        悲观创建文件夹，假定文件都没被创建，此时效率高，能直接越级创建深层级文件夹。
        :return:
        """
        path_dir_l = self.get_split_dir_path_list(path_dir)
        path_dir_l.reverse()
        need_make = []
        for root, folder in path_dir_l:
            try:
                if root:
                    self.ftp.mkd(root)
            except (ftplib.error_perm,) as inst:
                # print(root, str(ex))
                if get_err_str_setting('File exists', inst):
                    continue
                elif get_err_str_setting("Not a directory", inst):
                    raise SystemError(f'创建{self}文件夹时出现同名文件：{root}') from None
                else:
                    raise SystemError(f'创建{self}文件夹时出现问题：{root} {inst}')

        for root, folder in need_make:
            self.ftp.mkd(root)

    def upload_dir(self, dir_name, status_command=log_info, replace_command=log_info):
        # Change to directory
        os.chdir(dir_name)
        # Create directory in server and go inside
        try:
            if not self.is_there(dir_name):
                self.ftp.mkd(dir_name)
                status_command(dir_name, 'Creating directory')
            else:
                status_command(dir_name, 'Directory exists')
            self.ftp.cwd(dir_name)
        except:
            status_command(dir_name, 'Failed to create directory')
            return
        # Cycle through items
        for filename in os.listdir():
            # If file upload
            if isfile(filename):
                self.upload_file(filename, os.path.getsize(filename), status_command, replace_command)
            # If directory, recursive upload it
            else:
                self.upload_dir(filename, status_command, replace_command)

        # Got to parent directory
        self.ftp.cwd('..')
        os.chdir('..')

    def get_size_etc(self, ftp_file_name):
        old_path = self.work_dir_now
        path, _file_name = os.path.split(ftp_file_name)
        if path:
            self.cwd_recode_path(path)
        detailed_file_list = self.get_detailed_file_list(True)
        file_list = self.get_file_list(detailed_file_list)
        res = 1
        for file_name, file_details in zip(file_list, detailed_file_list):
            if file_name == _file_name and not self.is_dir(file_details):
                res = int(self.get_properties(file_details)[3])
        if path:
            self.cwd_recode_path(old_path)
        return res

    def walk(self, ftp_file_path):
        old_path = self.work_dir_now
        if ftp_file_path:
            if ftp_file_path.startswith('/'):
                ftp_file_path = os.path.realpath(ftp_file_path)
            else:
                ftp_file_path = os.path.realpath(os.path.join(self.work_dir_now, ftp_file_path))
            self.cwd_recode_path(ftp_file_path)
        else:
            raise FileNotFoundError(ftp_file_path)
        detailed_file_list = self.get_detailed_file_list(True)
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            r_fs = []
            r_fns = []
            pr = self.get_properties(file_details)
            file_name = pr[0]
            if file_name and file_name in ['.', '..']:
                continue
            if self.is_dir(file_details):
                r_fs.append(file_name)
                for root, fs, fns in self.walk(f"{ftp_file_path}/{file_name}"):
                    yield root, fs, fns
            else:
                file_attribs, date_modified = pr[1], pr[2]
                r_fns.append((file_name, file_attribs, date_modified, int(pr[-1] if pr[-1] else 0)))
            yield ftp_file_path, r_fs, r_fns

        if ftp_file_path:
            self.cwd_recode_path(old_path)

    def get_size(self, ftp_file_name):
        res = int(self.ftp.size(ftp_file_name)) or 0
        return res

    @with_ftp_lock()
    def get_size_until_success(self, ftp_file_name):
        res = int(self.ftp.size(ftp_file_name)) or 0
        return res

    def download_file_to_some_where(
            self, ftp_file_name, local_path, local_file_name='',
            file_size=0, status_command=log_info, replace_command=log_info, check_ftp_file_same=False
    ):
        if not local_path:
            raise SystemError(f'路径错误：{local_path.__repr__()}')
        # Variable to keep track of total bytes downloaded
        # Check if the file is already present in local directory
        if isfile(ftp_file_name):
            if replace_command(ftp_file_name, 'File exists in destination folder') is False:
                return
        # Try to open file, if fails return
        if not local_file_name:
            local_file_name = os.path.basename(ftp_file_name)

        makedirs(local_path, check_dot=False)
        local_path = os.path.join(local_path, local_file_name)

        # if local_path.endswith('/'):
        #     makedirs(local_path)
        #     local_path = os.path.join(local_path, local_file_name)
        # else:
        #     makedirs(os.path.dirname(local_path))

        if not file_size:
            file_size = self.get_size_until_success(ftp_file_name)

        self._download_file_to_some_where(
            ftp_file_name, local_path, file_size, status_command, check_ftp_file_same=check_ftp_file_same)
        return local_path

    @with_ftp_lock()
    def _download_file_to_some_where(
            self, ftp_file_name, local_path,
            file_size, status_command=log_info, check_ftp_file_same=False
    ):
        self.bytes_downloaded = 0
        self.is_same_file = False
        self.down_file_size_start = 0

        self.last_log = 0

        # Function for updating status and writing to file

        def write_file(data):
            self.bytes_downloaded += int(sys.getsizeof(data))
            all_size = f'{file_size / 1024 / 1024:.3f}MB'
            down_size = f'{(self.down_file_size_start + self.bytes_downloaded) / 1024 / 1024:.3f}MB'
            per_second_size = f'{self.bytes_downloaded / 1024 / 1024 / (time.time() - start_time):.3f}MB/s'
            if self.last_log + 2 < (time_now := time.time()):
                self.last_log = time_now
                status_command(ftp_file_name,
                               str(min(
                                   round(((self.down_file_size_start + self.bytes_downloaded) / file_size) * 100, 3),
                                   100)) + f'% [{down_size}/ {all_size}] {per_second_size}')
            file_to_down.write(data)

        def check_file(data):
            bytes_downloaded = int(sys.getsizeof(data))
            all_size = f'{file_size / 1024 / 1024:.3f}MB'
            down_size = f'{bytes_downloaded / 1024 / 1024:.3f}MB'
            per_second_size = f'{bytes_downloaded / 1024 / 1024 / (time.time() - start_time):.3f}MB/s'
            status_command(
                '正在检查本地已经存在的文件路径下载的文件大小', ftp_file_name, str(min(round(
                    (bytes_downloaded / file_size) * 100, 3), 100)) + f'% [{down_size}/ {all_size}] {per_second_size}')

            f_data = file_need_down.read(len(data))
            status_command(f"比较服务器和本地文件一致性：\n{f_data[:200]}\n{data[:200]}\n{len(f_data)}=?{len(data)}:{f_data == data}")
            if f_data == data:
                self.is_same_file = True
                self.down_file_size_start = os.path.getsize(local_path)
                status_command(f"已经下载的文件大小:{self.down_file_size_start / 1024 / 1024:.3f}MB", )
            else:
                self.is_same_file = False
                raise SystemError(f'本地文件路径["{local_path}:1"]已存在，而且文件开头与服务器不一致，请检查')
            raise StopIteration('只是检查文件开头一致性，不需要全部下载')

        start_time = time.time()
        if os.path.exists(local_path) and os.path.getsize(local_path):
            if file_size and file_size == os.path.getsize(local_path):
                status_command(f"已经下载的文件大小 与 传入的文件大小参数 一致，跳过下载:{file_size / 1024 / 1024:.3f}MB", )
                return
            if check_ftp_file_same:
                with open(local_path, 'rb') as file_need_down:
                    status_command(
                        f'正在下载文件 [{file_size / 1024 / 1024:.3f}MB] {ftp_file_name} --> {os.path.realpath(local_path)}')
                    try:
                        self.ftp.retrbinary('RETR ' + ftp_file_name, check_file, blocksize=1024 * 8 * 128)  # 检查800KB
                    except StopIteration:
                        try:
                            status_command(self.ftp.getmultiline())
                            self.ftp.abort()
                        except NETWORK_ERR:
                            self.ftp.close()
                            self.connect_until_success()
            else:
                self.is_same_file = True
                self.down_file_size_start = os.path.getsize(local_path)

        start_time = time.time()
        with open(local_path, 'ab') as file_to_down:
            status_command(ftp_file_name,
                           f'Downloading file[{self.down_file_size_start / 1024 / 1024}MB/{file_size / 1024 / 1024:.3f}MB] to {os.path.realpath(local_path)}')
            self.ftp.retrbinary('RETR ' + ftp_file_name, write_file, rest=self.down_file_size_start or None)
            status_command(ftp_file_name,
                           f'Download success [{time.time() - start_time}s {file_size / 1024 / 1024:.3f}MB] to {os.path.realpath(local_path)}')

    def download_file(self, ftp_file_name, file_size=0, status_command=log_info, replace_command=log_info):
        # Function for updating status and writing to file
        def write_file(data):
            self.bytes_downloaded += int(sys.getsizeof(data))
            status_command(ftp_file_name, str(min(round((self.bytes_downloaded / file_size) * 100, 3), 100)) + '%')
            file_to_down.write(data)

        # Variable to keep track of total bytes downloaded
        self.bytes_downloaded = 0
        # Check if the file is already present in local directory
        if isfile(ftp_file_name):
            if replace_command(ftp_file_name, 'File exists in destination folder') is False:
                return
        # Try to open file, if fails return
        try:
            file_to_down = open(ftp_file_name, 'wb')
        except:
            status_command(ftp_file_name, 'Failed to create file')
            return
        # Try to upload file
        try:
            status_command(ftp_file_name, 'Downloading')
            self.ftp.retrbinary('RETR ' + ftp_file_name, write_file)
            status_command(None, 'newline')
        except:
            status_command(ftp_file_name, 'Download failed')
        # Close file
        file_to_down.close()

    def download_dir(self, ftp_dir_name, status_command=log_info, replace_command=log_info):
        # Create local directory
        try:
            if not os.path.isdir(ftp_dir_name):
                os.makedirs(ftp_dir_name)
                status_command(ftp_dir_name, 'Created local directory')
            else:
                status_command(ftp_dir_name, 'Local directory exists')
            os.chdir(ftp_dir_name)
        except:
            status_command(ftp_dir_name, 'Failed to create local directory')
            return
        # Go into the ftp directory
        self.ftp.cwd(ftp_dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list(True)
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If directory
            if self.is_dir(file_details):
                self.download_dir(file_name, status_command, replace_command)
            # If file
            else:
                self.download_file(file_name, int(self.get_properties(file_details)[3]), status_command,
                                   replace_command)
        # Got to parent directory
        self.ftp.cwd('..')
        os.chdir('..')

    def search(self, dir_name, search_file_name, status_command=log_info):
        # Go into the ftp directory
        self.ftp.cwd(dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list()
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If file_name matches the keyword, append it to search list
            if search_file_name.lower() in file_name.lower():
                if self.ftp.pwd() == '/':
                    dir_ = ''
                else:
                    dir_ = self.ftp.pwd()
                self.search_file_list.append(dir_ + '/' + file_name)
                self.detailed_search_file_list.append(file_details)
                status_command(dir_ + '/' + file_name, 'Found')
                # If directory, search it
            if self.is_dir(file_details):
                status_command(file_name, 'Searching directory')
                self.search(file_name, search_file_name, status_command)
        # Goto to parent directory
        self.ftp.cwd('..')

    def clear_search_list(self):
        del self.search_file_list[:]
        del self.detailed_search_file_list[:]

    def get_dir_size(self, dir_name):
        size = 0
        # Go into the ftp directory
        self.ftp.cwd(dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list()
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            if self.is_dir(file_details):
                size += self.get_dir_size(file_name)
            else:
                size += int(self.get_properties(file_details)[3])
        # Goto to parent directory
        self.ftp.cwd('..')
        return size

    def cwd_parent(self, name):
        if '/' not in name:
            return name
        parent_name = '/'.join(name.split('/')[:-1])
        if parent_name == '':
            parent_name = '/'
        self.ftp.cwd(parent_name)
        return ''.join(name.split('/')[-1:])

    def mkd(self, name):
        self.ftp.mkd(name)

    def pwd(self):
        return self.ftp.pwd()

    def get_properties(self, file_details):
        if self.server_platform == 'Linux':
            details_list = file_details.split()
            # Get file attributes
            file_attribs = details_list[0]
            # Get date modified
            date_modified = ' '.join(details_list[5:8])
            # Remove the path from the name
            file_name = ' '.join(details_list[8:])
            # Get size if it is not a directory
            if 'd' not in file_details[0]:
                file_size = details_list[4]
                return [file_name, file_attribs, date_modified, file_size]
            else:
                return [file_name, file_attribs, date_modified]

    def is_dir(self, file_details):
        if self.server_platform == 'Linux':
            return 'd' in file_details[0]

    @with_ftp_lock()
    def cwd_recode_path(self, path):
        self.ftp.cwd(path)
        if path.startswith('/'):
            self.work_dir_now = os.path.realpath(path)
        else:
            old_path = self.work_dir_now
            self.work_dir_now = os.path.realpath(os.path.join(old_path, path))

    def moveFTPFiles(self, fileToUpload, remoteDirectoryPath):
        self.lock.acquire(True)
        try:
            localFileName = path_leaf(fileToUpload)
            if localFileName != "":
                # create a copy of the file before sending it
                # tempFile = create_temporary_copy(fileToUpload, workingDir, localFileName)
                # open a the local file
                with open(fileToUpload, 'rb') as rf:
                    wf = TemporaryFile('w+b')
                    while datar := rf.read(8128):
                        wf.write(datar)
                    wf.seek(0)
                    # Download the file a chunk at a time using RETR

                    # p_start = self.pwd()
                    p_start = self.work_dir_now
                    self.cwd(remoteDirectoryPath)
                    self.ftp.storbinary('STOR ' + localFileName, wf)
                    self.cwd(p_start)

                    wf.close()
                logger.info("Uploaded file: " + str(fileToUpload) + " on " + time_stamp())

                # Close the file
                # remove the temp file
                # to remove it I need to have the right permissions
                # os.chmod(tempFile, 777)
                # os.remove(tempFile)
        except Exception as inst:
            logger.exception(f"Connection Error - {inst}" + time_stamp())

        self.lock.release()


class FtpClientFileStore(midhardware.BaseStore):
    # TODO
    pass


class FtpClientStore(midhardware.BaseStore):
    def __init__(self, host, port, username, password, location='/', tmp_path='ftp_data_tmp',
                 use_tls=False, pasv=None, encoding='utf-8', socks_proxy: typing.Union[str, tuple, dict] = None,
                 download_check_ftp_file_same=False, upload_check_ftp_file_same=True):
        if isinstance(socks_proxy, str):
            p_host, p_port = socks_proxy.split(':')
        elif isinstance(socks_proxy, tuple):
            p_host, p_port = socks_proxy
            socks_proxy = dict(proxytype=socks.PROXY_TYPE_SOCKS5, rdns=True, addr=p_host, port=int(p_port))
        self.client = FtpController(host, port, username, password, use_tls, pasv, encoding, socks_proxy)
        self.client.connect_until_success()
        self.location = location
        self.download_check_ftp_file_same = download_check_ftp_file_same
        self.upload_check_ftp_file_same = upload_check_ftp_file_same
        self.tmp_path = tmp_path
        self.client.cwd_recode_path(self.location)

    def count_data(self, data_type=None, *args, **kwargs):
        return NotImplementedError

    def list_data(self, data_type=None, location=None, *args, **kwargs):
        for root, fs, fns in self.client.walk(location or self.location):
            for fn, file_attribs, date_modified, size in fns:
                yield {
                    'root': root,
                    'filename': fn,
                    'attribs': file_attribs,
                    'modified': date_modified,
                    'size': int(size),
                    "realpath": os.path.realpath(os.path.join(root, fn))
                }

    def check_data(self, position: typing.Union[str, dict, FtpClientFileStore], data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_data(self, position: typing.Union[str, dict, FtpClientFileStore], data_type=None, *args, **kwargs):
        """

        self.tmp_path 本地文件夹路径
        本地文件名不用也不能传 TODO 可选重命名
        position 远程文件名
        self.location 远程文件夹路径
        """
        file_name = None
        file_size = 0
        if isinstance(position, str):
            file_name = os.path.join(self.location, position)
        elif isinstance(position, dict):
            file_name = position['realpath']
            file_size = position['size']
        elif isinstance(position, FtpClientFileStore):
            pass
        if file_name:
            return self.client.download_file_to_some_where(
                file_name,
                self.tmp_path,
                file_size=file_size,
                check_ftp_file_same=self.download_check_ftp_file_same
            )

    def save_data(self, position: str, data: typing.Union[str, bytes, io.BytesIO], data_type=None, *args, **kwargs):
        if isinstance(data, str):
            data = data
        elif isinstance(data, (bytes, io.BytesIO)):
            raise NotImplementedError
        else:
            raise SystemError(f'data type error: {type(data)}')
        return self.client.upload_file_to_some_where(
                data,
                self.location,
                position,
                check_ftp_file_same=self.upload_check_ftp_file_same
            )

    def delete_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_position(self, position, data_type=None, *args, **kwargs):
        return os.path.join(self.location, position)

    def get_data_size(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def check_self(self, *args, **kwargs):
        return NotImplementedError

    def save_self(self, *args, **kwargs):
        return NotImplementedError

    def free_self(self, *args, **kwargs):
        return NotImplementedError


class FtpSeverStore(midhardware.BaseStore):
    def __init__(
            self, host='0.0.0.0', port=21, username='user', password="123qwe", location='/home/user',
            use_tls=False, pasv=True, encoding='utf-8'):
        authorizer = DummyAuthorizer()
        authorizer.add_user(username, password, location, perm=password)
        handler = FTPHandler
        handler.authorizer = authorizer

        server = FTPServer((host, port), handler)
        server.serve_forever()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.location = location

    def count_data(self, data_type=None, *args, **kwargs):
        return NotImplementedError

    def list_data(self, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def save_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def delete_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_position(self, position, data_type=None, *args, **kwargs):
        return os.path.join(self.location, position)

    def get_data_size(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def check_self(self, *args, **kwargs):
        return NotImplementedError

    def save_self(self, *args, **kwargs):
        return NotImplementedError

    def free_self(self, *args, **kwargs):
        return NotImplementedError


if __name__ == '__main__':
    __fs = FtpClientStore('127.0.0.1', 21111, 'python', '123456', '/',
                          use_tls=False, pasv=True, encoding='utf-8', socks_proxy=None)
    __fs.client.try_all_connect_to()
    __fs.client.connect_until_success()
    __fs.client.ftp.cwd("123f")
    __fs.client.make_dir_optimistic('/testss1/test1/tests11/wfwe/rdg24/asdfg')
    __fs.client.check_encoding()

    # for __f in __fs.client.get_detailed_file_list():
    #     print(__f)
