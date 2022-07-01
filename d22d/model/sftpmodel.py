# whipFTP, Copyrights Vishnu Shankar B,

import os
import re
import time
import logging
from os import listdir
from os.path import isfile, join
import shutil
import paramiko
import socks
import socket
import sys
import typing

from d22d.model import midhardware
from d22d.utils import log_info
from d22d.utils.ziputils import makedirs

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ParamikoSftpClient(paramiko.SFTPClient):
    def cwd(self, path):
        self.chdir(path)

    def go_to_home(self, username):
        try:
            self.cwd('/home/' + username)
        except:
            self.cwd('/')

NETWORK_ERR = (
    socket.timeout,  # 直接断了
    EOFError,  # 用代理断了
    socks.ProxyConnectionError,  # 用代理的时候被拔网线
    OSError  # 被拔网线
)

class SftpController:
    # /!\ Although the comments and variable names say 'file_name'/'file_anything' it inculdes folders also
    # Some functions in this class has no exception handling, it has to be done outside

    def __init__(self, host, port=22, username=' ', password=' '):
        # List to store file search and search keywords
        self.search_file_list = []
        self.detailed_search_file_list = []
        self.keyword_list = []

        # Variable to hold the max no character name in file list (used for padding in GUIs)
        self.max_len = 0
        self.max_len_name = ''

        # Variable to tell weather hidden files are enabled
        self.hidden_files = False

        self.work_dir_now = '/'

        self.host = host
        self.username = username
        self.password = password
        self.port = port

    def connect_until_success(self, retry=0):
        cnt_retry = 0
        while True:
            cnt_retry += 1
            try:
                log_info(f"正在尝试第[{cnt_retry}/{retry}]次登录[{self}]。。。")
                # 先无tls，在试试有tls
                self.ftp = None
                self.transport = paramiko.Transport((self.host, self.port))
                self.transport.connect(username=self.username, password=self.password)
                self.ftp = ParamikoSftpClient.from_transport(self.transport)
                return
            except Exception as inst:
                if retry and cnt_retry > retry:
                    raise inst
                print_func = logger.error
                if not isinstance(inst, NETWORK_ERR):
                    print_func = logger.exception
                print_func(f"第[{cnt_retry}/{retry}]次登录[{self}]失败 [{type(inst)}] {inst}, 5秒后重试。。。")
                time.sleep(5)

    def connect_to(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.username, password=self.password)
        self.ftp = ParamikoSftpClient.from_transport(self.transport)
        # self.ftp.go_to_home(self.username)

    def toggle_hidden_files(self):
        self.hidden_files = not self.hidden_files

    def cwd_recode_path(self, path):
        self.ftp.cwd(path)
        if path.startswith('/'):
            self.work_dir_now = self.format_realpath(path)
        else:
            old_path = self.work_dir_now
            self.work_dir_now = self.format_realpath(os.path.join(old_path, path))

    def walk(self, ftp_file_path):
        old_path = self.work_dir_now
        if ftp_file_path:
            if ftp_file_path.startswith('/'):
                ftp_file_path = self.format_realpath(ftp_file_path)
            else:
                ftp_file_path = self.format_realpath(os.path.join(self.work_dir_now, ftp_file_path))
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

    def get_detailed_file_list(self, ignore_hidden_files_flag=False):
        files = []
        for attr in self.ftp.listdir_attr():
            if (self.hidden_files is True or str(attr).split()[8][0] is not '.') or ignore_hidden_files_flag is True:
                files.append(str(attr))
        return files

    def get_file_list(self, detailed_file_list):
        self.max_len = 0
        self.max_len_name = ''
        file_list = []
        for x in detailed_file_list:
            # Remove details and append only the file name
            name = ' '.join(x.split()[8:])
            file_list.append(name)
            if (len(name) > self.max_len):
                self.max_len = len(name)
                self.max_len_name = name
        return file_list

    def get_detailed_search_file_list(self):
        return self.detailed_search_file_list

    def get_search_file_list(self):
        self.max_len = 0
        self.max_len_name = ''
        for name in self.search_file_list:
            if (len(name) > self.max_len):
                self.max_len = len(name)
                self.max_len_name = name
        return self.search_file_list

    def chmod(self, filename, permissions):
        self.ftp.chmod(filename, permissions)

    @staticmethod
    def format_realpath(path):
        if sys.platform == 'win32':
            res = str(os.path.realpath(path)).split(':', 1)[-1].replace('\\', '/')
        else:
            res = os.path.realpath(path)
        return res

    @staticmethod
    def format_path(path):
        if sys.platform == 'win32':
            res = str(path).replace('\\', '/')
        else:
            res = path
        return res

    def is_there(self, path):
        try:
            self.ftp.stat(path)
            return True
        except:
            return False

    def rename_dir(self, rename_from, rename_to):
        self.ftp.rename(rename_from, rename_to)

    def move_dir(self, rename_from, rename_to, status_command, replace_command):
        if (self.is_there(rename_to) is True):
            if (replace_command(rename_from, 'File/Folder exists in destination folder') is True):
                self.delete_dir(rename_to, status_command)
            else:
                return
        try:
            self.ftp.rename(rename_from, rename_to)
            status_command(rename_from, 'Moved')
        except:
            status_command(rename_from, 'Failed to move')
            raise

    def copy_file(self, file_dir, copy_from, file_size, status_command, replace_command):
        # Change to script's directory
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        if not os.path.exists('copy_temps'):
            os.makedirs('copy_temps')
        os.chdir('copy_temps')
        # Save the current path so that we can copy later
        dir_path_to_copy = self.ftp.getcwd()
        # Change to the file's path and download it
        self.ftp.cwd(file_dir)
        self.download_file(copy_from, file_size, status_command, replace_command)
        # Change back to the saved path and upload it
        self.ftp.cwd(dir_path_to_copy)
        self.upload_file(copy_from, file_size, status_command, replace_command)
        # Delete the downloaded file
        os.remove(copy_from)
        status_command(copy_from, 'Deleted local file')

    def copy_dir(self, file_dir, copy_from, status_command, replace_command):
        # Change to script's directory
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        if not os.path.exists('copy_temps'):
            os.makedirs('copy_temps')
        os.chdir('copy_temps')
        # Save the current path so that we can copy later
        dir_path_to_copy = self.ftp.getcwd()
        # Change to the file's path and download it
        self.ftp.cwd(file_dir)
        self.download_dir(copy_from, status_command, replace_command)
        # Change back to the saved path and upload it
        self.ftp.cwd(dir_path_to_copy)
        self.upload_dir(copy_from, status_command, replace_command)
        # Delete the downloaded folder
        shutil.rmtree(copy_from)
        status_command(copy_from, 'Deleting local directory')

    def delete_file(self, file_name, status_command):
        try:
            self.ftp.remove(file_name)
            status_command(file_name, 'Deleted')
        except:
            status_command(file_name, 'Failed to delete')
            raise

    def delete_dir(self, dir_name, status_command):
        # Go into the directory
        self.ftp.cwd(dir_name)
        # Get file lists
        try:
            detailed_file_list = self.get_detailed_file_list(True)
        except:
            status_command(dir_name, 'Failed to delete directory')
            raise
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If directory
            if (self.is_dir(file_details)):
                self.delete_dir(file_name, status_command)
            # If file
            else:
                self.delete_file(file_name, status_command)
        # Go back to parent directory and delete it
        try:
            self.ftp.cwd('..')
            status_command(dir_name, 'Deleting directory')
            self.ftp.rmdir(dir_name)
        except:
            status_command(dir_name, 'Failed to delete directory')
            raise

    def upload_file(self, file_name, file_size, status_command, replace_command):
        # Function to update status
        def upload_progress(transferred, remaining):
            status_command(file_name, str(min(round((transferred / file_size) * 100, 8), 100)) + '%')

        # Check if the file is already present in ftp server
        if (self.is_there(file_name)):
            if (replace_command(file_name, 'File exists in destination folder') is False):
                return
        # Try to upload file
        try:
            status_command(file_name, 'Uploading')
            self.ftp.put(file_name, file_name, callback=upload_progress)
            status_command(None, 'newline')
        except:
            status_command(file_name, 'Upload failed')
            raise

    def upload_dir(self, dir_name, status_command, replace_command):
        # Change to directory
        os.chdir(dir_name)
        # Create directory in server and go inside
        try:
            if (not self.is_there(dir_name)):
                self.ftp.mkdir(dir_name)
                status_command(dir_name, 'Creating directory')
            else:
                status_command(dir_name, 'Directory exists')
            self.ftp.cwd(dir_name)
        except:
            status_command(dir_name, 'Failed to create directory')
            raise
        # Cycle through items
        for filename in os.listdir():
            # If file upload
            if (isfile(filename)):
                self.upload_file(filename, os.path.getsize(filename), status_command, replace_command)
            # If directory, recursive upload it
            else:
                self.upload_dir(filename, status_command, replace_command)

        # Got to parent directory
        self.ftp.cwd('..')
        os.chdir('..')

    def sftp_mkdir_p(self, remote_path):
        if remote_path == "/":
            # absolute path so change directory to root
            self.ftp.chdir("/")
            return
        if remote_path == "":
            # top-level relative directory must exists
            return
        try:
            # sub-directory exists
            self.ftp.chdir(remote_path)
        except IOError:
            dirname, basename = os.path.split(remote_path.rstrip("/"))
            self.sftp_mkdir_p(dirname)
            self.ftp.mkdir(basename)
            self.ftp.chdir(basename)

    def upload_file_to_some_where(
            self, local_path, remote_folder, remote_filename='',
            status_command=log_info, check_ftp_file_same=False, append_offset=0):
        # TODO 断点续传
        if not os.path.exists(local_path):
            raise SystemError(f'本地路径不存在：{local_path.__repr__()}')
        if not remote_folder:
            raise SystemError(f'远程路径错误：{remote_folder.__repr__()} {remote_filename}')
        if not remote_filename:
            remote_folder, remote_filename = os.path.split(remote_folder)

        if not remote_folder.startswith('/'):
            remote_folder = self.format_realpath(os.path.join(self.work_dir_now, remote_folder))
        remote_path = os.path.join(remote_folder, remote_filename)
        old_path = self.work_dir_now
        if remote_folder:
            # Create directory in server and go inside
            try:
                if (not self.is_there(remote_folder)):
                    self.sftp_mkdir_p(remote_folder)
                    status_command(remote_folder, 'Creating directory')
                else:
                    status_command(remote_folder, 'Directory exists')
                self.ftp.cwd(remote_folder)
            except Exception as e:
                status_command(remote_folder, 'Failed to create directory')
                raise e

        self._upload_file_to_some_where(local_path, remote_path, status_command, check_ftp_file_same, append_offset)
        self.work_dir_now = old_path
        self.cwd_recode_path(old_path)

    def _upload_file_to_some_where(
            self, local_path, remote_path, status_command=log_info,
            check_ftp_file_same=False, append_offset=0, windows=1024 * 8 * 128):
        file_size = os.stat(local_path).st_size

        # Function to update status
        def upload_progress(transferred, remaining):
            status_command(local_path, str(min(round((transferred / file_size) * 100, 8), 100)) + '%')

        # Try to upload file
        try:
            status_command(local_path, 'Uploading')
            self.ftp.put(local_path, remote_path, callback=upload_progress)
            file_list = self.ftp.listdir(os.path.dirname(remote_path))
            remote_name = os.path.basename(remote_path)
            f_local = open(local_path)

            if remote_name in file_list:
                f_remote = self.ftp.open(remote_path, "a")
                stat = self.ftp.stat(remote_path)
                if check_ftp_file_same:
                    f_remote_tmp = self.ftp.open(remote_path, "r")
                    try:
                        r_data = f_remote_tmp.read(windows)
                        l_data = f_local.read(len(r_data))
                        status_command(
                            f"正在检查远程sftp服务器已经存在的文件路径上传的文件和本地文件一致性：\n",
                            f"本地：{l_data[:70]}\n远程：{r_data[:70]}\n本地{len(l_data)}=?远程{len(r_data)}: {l_data == r_data}")
                        if l_data == r_data:
                            self.is_same_file = True
                            status_command(
                                f"文件开头800KB一致，准备开始断点续传，已经上传的文件大小:{stat.st_size / 1024 / 1024:.3f}MB", )
                        else:
                            self.is_same_file = False
                            raise SystemError(f'远程文件路径["{self}{remote_path}:1"]已存在，而且本地文件开头800KB与服务器文件不一致，请检查')
                        raise StopIteration('只是检查文件开头一致性，不需要全部下载')
                    finally:
                        f_remote_tmp.close()

                stat = self.ftp.stat(remote_path)
                f_local.seek(stat.st_size)
                if append_offset:
                    f_remote.seek(append_offset)
            else:
                f_remote = self.ftp.open(remote_path, "w")

            tmp_buffer = f_local.read(windows)
            while tmp_buffer:
                f_remote.write(tmp_buffer)
                tmp_buffer = f_local.read(windows)
            f_remote.close()
            f_local.close()
            status_command(None, 'newline')
        except Exception as e:
            status_command(remote_path, f'Upload failed [{type(e)}] {e}')
            raise e

    def download_file(self, ftp_file_name, file_size, status_command, replace_command):
        # Function to update progress
        def download_progress(transferred, remaining):
            status_command(ftp_file_name, str(min(round((transferred / file_size) * 100, 8), 100)) + '%')

        # Check if the file is already present in local directory
        if isfile(ftp_file_name):
            if replace_command(ftp_file_name, 'File exists in destination folder') is False:
                return
        # Try to download file
        try:
            status_command(ftp_file_name, 'Downloading')
            self.ftp.get(ftp_file_name, ftp_file_name, callback=download_progress)
            status_command(None, 'newline')
        except Exception:
            status_command(ftp_file_name, 'Download failed')
            raise

    def download_dir(self, ftp_dir_name, status_command, replace_command):
        # Create local directory
        try:
            if not os.path.isdir(ftp_dir_name):
                os.makedirs(ftp_dir_name)
                status_command(ftp_dir_name, 'Created local directory')
            else:
                status_command(ftp_dir_name, 'Local directory exists')
            os.chdir(ftp_dir_name)
        except Exception:
            status_command(ftp_dir_name, 'Failed to create local directory')
            raise
            # Go into the ftp directory
        self.ftp.cwd(ftp_dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list(True)
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If directory
            if (self.is_dir(file_details)):
                self.download_dir(file_name, status_command, replace_command)
            # If file
            else:
                self.download_file(file_name, int(self.get_properties(file_details)[3]), status_command,
                                   replace_command)
        # Got to parent directory
        self.ftp.cwd('..')
        os.chdir('..')

    def get_size(self, ftp_file_name):
        res = int(self.ftp.stat(ftp_file_name).st_size) or 0
        return res

    def _download_file_to_some_where( self, ftp_file_name, local_path,
                                      file_size, status_command):
        # Function to update progress
        def download_progress(transferred, remaining):
            status_command(ftp_file_name, str(min(round((transferred / file_size) * 100, 8), 100)) + '%')

        # Try to download file
        try:
            status_command(ftp_file_name, 'Downloading')
            self.ftp.get(ftp_file_name, local_path, callback=download_progress)
            status_command(None, 'newline')
        except Exception as e:
            status_command(ftp_file_name, 'Download failed')
            raise e

    def download_file_to_some_where(self, ftp_file_name, local_path, local_file_name='',
                                    file_size=0, status_command=log_info, replace_command=log_info):
        if not local_path:
            raise SystemError(f'路径错误：{local_path.__repr__()}')
        if isfile(ftp_file_name):
            if replace_command(ftp_file_name, 'File exists in destination folder') is False:
                return
            # Try to open file, if fails return
        if not local_file_name:
            local_file_name = os.path.basename(ftp_file_name)

        makedirs(local_path, check_dot=False)
        local_path = self.format_path(os.path.join(local_path, local_file_name))

        if not file_size:
            file_size = self.get_size(ftp_file_name)

        self._download_file_to_some_where(ftp_file_name, local_path, file_size, status_command)
        return local_path

    def search(self, dir_name, status_command, search_file_name):
        # Go into the ftp directory
        self.ftp.cwd(dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list()
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            # If file_name matches the keyword, append it to search list
            if search_file_name.lower() in file_name.lower():
                if (self.ftp.getcwd() == '/'):
                    dir = ''
                else:
                    dir = self.ftp.getcwd()
                self.search_file_list.append(dir + '/' + file_name)
                self.detailed_search_file_list.append(file_details)
                status_command(dir + '/' + file_name, 'Found')
                # If directory, search it
            if (self.is_dir(file_details)):
                status_command(file_name, 'Searching directory')
                self.search(file_name, status_command, search_file_name)
        # Goto to parent directory
        self.ftp.cwd('..')

    def clear_search_list(self):
        del self.search_file_list[:]
        del self.detailed_search_file_list[:]

    def get_dir_size(self, dir_name):
        size = 0;
        # Go into the ftp directory
        self.ftp.cwd(dir_name)
        # Get file lists
        detailed_file_list = self.get_detailed_file_list()
        file_list = self.get_file_list(detailed_file_list)
        for file_name, file_details in zip(file_list, detailed_file_list):
            if (self.is_dir(file_details)):
                size += self.get_dir_size(file_name)
            else:
                size += int(self.get_properties(file_details)[3])
        # Goto to parent directory
        self.ftp.cwd('..')
        # return size
        return size

    def cwd_parent(self, name):
        if ('/' not in name): return name
        parent_name = '/'.join(name.split('/')[:-1])
        if (parent_name == ''): parent_name = '/'
        self.ftp.cwd(parent_name)
        return ''.join(name.split('/')[-1:])

    def mkd(self, name):
        self.ftp.mkdir(name)

    def pwd(self):
        return (self.ftp.getcwd())

    def get_properties(self, file_details):
        details_list = file_details.split()
        # Get file attributes
        file_attribs = details_list[0]
        # Get date modified
        date_modified = ' '.join(details_list[5:8])
        # Remove the path from the name
        file_name = ' '.join(details_list[8:])
        # Get size if it is not a directory
        if ('d' not in file_details[0]):
            file_size = details_list[4]
            return [file_name, file_attribs, date_modified, file_size]
        else:
            return [file_name, file_attribs, date_modified]

    def is_dir(self, file_details):
        return 'd' in file_details[0]

    def disconnect(self):
        if self.ftp:
            self.ftp.close()


class ParamikoFolderUploader(object):
    """
    paramoki 实现的文件夹上传
    """

    def __init__(self, host, port, user, password, local_dir: str, remote_dir: str,
                 path_pattern_exluded_tuple=('/.git/', '/.idea/', '/dist/', '/build/'),
                 file_suffix_tuple_exluded=('.pyc', '.log', '.gz'),
                 only_upload_within_the_last_modify_time=3650 * 24 * 60 * 60,
                 file_volume_limit=1000 * 1000, sftp_log_level=20):
        """
        :param host:
        :param port:
        :param user:
        :param password:
        :param local_dir:
        :param remote_dir:
        :param path_pattern_exluded_tuple: 命中了这些正则的直接排除
        :param file_suffix_tuple_exluded: 这些结尾的文件排除
        :param only_upload_within_the_last_modify_time: 仅仅上传最近多少天修改的文件
        :param file_volume_limit: 大于这个体积的不上传，单位b。
        """
        self._host = host
        self._port = port
        self._user = user
        self._password = password

        self._local_dir = str(local_dir).replace('\\', '/')
        if not self._local_dir.endswith('/'):
            self._local_dir += '/'
        self._remote_dir = str(remote_dir).replace('\\', '/')
        if not self._remote_dir.endswith('/'):
            self._remote_dir += '/'
        self._path_pattern_exluded_tuple = path_pattern_exluded_tuple
        self._file_suffix_tuple_exluded = file_suffix_tuple_exluded
        self._only_upload_within_the_last_modify_time = only_upload_within_the_last_modify_time
        self._file_volume_limit = file_volume_limit

        # noinspection PyTypeChecker
        t = paramiko.Transport((host, port))
        t.connect(username=user, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(t)

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, port=port, username=user, password=password, compress=True)
        self.ssh = ssh

    def _judge_need_filter_a_file(self, filename: str):
        ext = filename.split('.')[-1]
        if '.' + ext in self._file_suffix_tuple_exluded:
            return True
        for path_pattern_exluded in self._path_pattern_exluded_tuple:
            # print(path_pattern_exluded,filename)
            if re.search(path_pattern_exluded, filename):
                return True
        file_st_mtime = os.stat(filename).st_mtime
        volume = os.path.getsize(filename)
        if time.time() - file_st_mtime > self._only_upload_within_the_last_modify_time:
            return True
        if volume > self._file_volume_limit:
            return True
        return False

    def _make_dir(self, dirc, final_dir):
        """
        sftp.mkdir 不能直接越级创建深层级文件夹。
        :param dirc:
        :param final_dir:
        :return:
        """
        # print(dir,final_dir)
        try:
            self.sftp.mkdir(dirc)
            if dirc != final_dir:
                self._make_dir(final_dir, final_dir)
        except (FileNotFoundError,):
            parrent_dir = os.path.split(dirc)[0]
            self._make_dir(parrent_dir, final_dir)

    def upload(self):
        for parent, dirnames, filenames in os.walk(self._local_dir):
            for filename in filenames:
                file_full_name = os.path.join(parent, filename).replace('\\', '/')
                if not self._judge_need_filter_a_file(file_full_name):
                    remote_full_file_name = re.sub(f'^{self._local_dir}', self._remote_dir, file_full_name)
                    try:
                        logger.debug(f'正在上传文件，本地：{file_full_name}  --> 远程： {remote_full_file_name}')
                        self.sftp.put(file_full_name, remote_full_file_name)
                    except (FileNotFoundError,) as e:
                        # self.logger.warning(remote_full_file_name)
                        self._make_dir(os.path.split(remote_full_file_name)[0], os.path.split(remote_full_file_name)[0])
                        self.sftp.put(file_full_name, remote_full_file_name)
                else:
                    if '/.git' not in file_full_name and '.pyc' not in file_full_name:
                        logger.debug(f'根据过滤规则，不上传这个文件 {file_full_name}')


class SftpClientStore(midhardware.BaseStore):
    def __init__(
            self, host, port, user, password, location='/', tmp_path='ftp_data_tmp',
            download_check_ftp_file_same=False, upload_check_ftp_file_same=False):
        self._host = host
        self._port = port
        self._user = user
        self._password = password

        self.tmp_path = tmp_path
        self.location = str(location).replace('\\', '/')
        if not self.location.endswith('/'):
            self.location += '/'

        self.client = SftpController(host, port, user, password)
        self.client.connect_until_success()
        self.client.sftp_mkdir_p(self.location)
        self.client.ftp.cwd(self.location)
        self.client.work_dir_now = self.location

        self.upload_check_ftp_file_same = upload_check_ftp_file_same
        self.download_check_ftp_file_same = download_check_ftp_file_same

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
                    "realpath": self.client.format_realpath(os.path.join(root, fn))
                }

    def check_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_data(self, position: typing.Union[str, dict], data_type=None, *args, **kwargs):
        """
        position 远程文件名
        self.tmp_path 本地文件夹路径
        self.location 远程文件夹路径
        """
        file_name = None
        file_size = 0
        if isinstance(position, str):
            file_name = self.client.format_path(os.path.join(self.location, position))
        elif isinstance(position, dict):
            file_name = position['realpath']
            file_size = position['size']
        if file_name:
            return self.client.download_file_to_some_where(
                file_name,
                self.tmp_path,
                file_size=file_size
            )

    def save_data(self, position: str, data, data_type=None, append_offset=0, *args, **kwargs):
        return self.client.upload_file_to_some_where(
                data,
                self.location,
                position,
                check_ftp_file_same=self.upload_check_ftp_file_same,
                append_offset=append_offset
            )

    def delete_data(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_position(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def get_data_size(self, position, data_type=None, *args, **kwargs):
        return NotImplementedError

    def check_self(self, *args, **kwargs):
        return NotImplementedError

    def save_self(self, *args, **kwargs):
        return NotImplementedError

    def free_self(self, *args, **kwargs):
        return NotImplementedError


if __name__ == '__main__':
    __fs = SftpClientStore('192.168.0.111', 57522, 'test', '1234qwer!@#$QWER', '/home/test', 'data')

    for __f in __fs.list_data():
        print(__f)
    # res = __fs.get_data('mysql2ftp_0424_1650809830.csv')
    res = __fs.save_data('mysql2ftp_0424_1650809830.csv','data/mysql2ftp_0424_1650809830.csv')
