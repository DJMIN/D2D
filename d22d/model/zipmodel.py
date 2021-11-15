import os
from io import BufferedIOBase
import zipfile
from shutil import rmtree, move

from d22d.utils import get_file_md5


class _DisableSeekAndTellIOWrapper(BufferedIOBase):
    """
    zipfile压缩文件的时候会通过seek和tell来定位写入zip文件头信息。
    但是管道流不能正常的seek和tell，故通过这个wrapper来禁用seek和tell，从而强迫zipfile使用流式压缩
    """

    def __init__(self, towrap):
        self._wrapped = towrap

    def seek(self, *args, **kwargs):
        raise AttributeError()

    def tell(self, *args, **kwargs):
        raise AttributeError()

    def seekable(self, *args, **kwargs):
        return False

    def __getattribute__(self, item):
        if item in ('_wrapped', 'seek', 'seekable', 'tell'):
            return object.__getattribute__(self, item)
        return self._wrapped.__getattribute__(item)

    def __enter__(self):
        self._wrapped.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self._wrapped.__exit__(*args, **kwargs)


def _zip_directory_to_pipe(dir_path: str) -> BufferedIOBase:
    rfd, wfd = os.pipe()

    def zip_in_thread(wfd):
        with os.fdopen(wfd, 'wb') as outputfile:
            with zipfile.ZipFile(_DisableSeekAndTellIOWrapper(outputfile), 'w') as zfile:
                for root, dirs, files in os.walk(dir_path):
                    for name in files:
                        fullpath = os.path.join(root, name)
                        print(fullpath)
                        arcname = os.path.relpath(fullpath, dir_path)
                        zfile.write(fullpath, arcname=arcname)

    import threading
    threading.Thread(target=zip_in_thread, args=(wfd,), name='zip ' + dir_path).start()
    return os.fdopen(rfd, 'rb')


def iter_zip_data(save_path, file_path):
    # import requests
    #
    # requests.post('http://localhost:8080/upload', files={'file': _zip_directory_to_pipe('d:/testdir')})
    with open(f'{save_path}.zip', 'wb') as wwf:
        cnt = 0
        while data := _zip_directory_to_pipe(file_path).read(8192):
            wwf.write(data)
            cnt += len(data)
            if not cnt % 10000:
                print(cnt)
                wwf.flush()


def zip_by_volume(file_path, block_size=1024 * 1024 * 50, ser='linux'):  # 分卷大小 50MB
    """zip单文件分卷压缩
    会删除文件所在目录名为：“文件名+_zip_split”的文件夹，再重新创建，在里面创建分卷
    分卷命名规则：末尾会用：“_分卷前文件MD5_分卷数.zip.001” -> “_分卷前文件MD5_分卷数.zip.999”
    返回目录路径和文件数量
    """
    file_size = os.path.getsize(file_path)  # 文件字节数
    count = file_size // block_size + 1

    folder_path, file_name = os.path.split(file_path)  # 除去文件名以外的path，文件名
    file_name_base = file_name.split('.')[0]  # 文件后缀名
    save_dir = os.path.join(folder_path, file_name) + '_zip_split'
    save_path = os.path.join(save_dir, file_name_base + f'_{int(count)}.zip')
    # 创建分卷压缩文件的保存路径
    if os.path.exists(save_dir):
        rmtree(save_dir)
    os.makedirs(save_dir)

    # 添加到临时压缩文件
    with zipfile.ZipFile(save_path, 'w') as zf:
        zf.write(file_path, arcname=file_name)
    md5_value = get_file_md5(save_path)
    # 小于分卷尺寸则直接返回压缩文件路径
    if file_size <= block_size:
        last_path = os.path.join(save_dir, file_name_base + f'_{md5_value}_1.zip')
        move(save_path, last_path)
        return save_dir, last_path, 1
    else:
        fp = open(save_path, 'rb')
        save_dir_last = f"{file_name_base}._{md5_value}_{count}.zip"
        if ser == 'win':
            for i in range(1, count + 1):
                _suffix = f'_{md5_value}_{count}.z{i:0>2}' if i != count else f'_{md5_value}_{count}.zip'
                name = os.path.join(save_dir, f"{file_name_base}.{_suffix}")
                f = open(name, 'wb+')
                if i == 1:
                    f.write(b'\x50\x4b\x07\x08')  # 添加分卷压缩header(4字节)
                    f.write(fp.read(block_size - 4))
                else:
                    f.write(fp.read(block_size))
        elif ser == 'linux':
            # 拆分压缩包为分卷文件
            for i in range(1, count + 1):
                _suffix = f'_{md5_value}_{count}.zip.{i:0>3}'
                name = os.path.join(save_dir, f"{file_name_base}.{_suffix}")
                f = open(name, 'wb+')
                if i == 1:
                    f.write(b'\x50\x4b\x07\x08')  # 添加分卷压缩header(4字节)
                    f.write(fp.read(block_size - 4))
                else:
                    f.write(fp.read(block_size))
        else:
            # 拆分压缩包为分卷文件
            for i in range(1, count + 1):
                _suffix = f'_{md5_value}_{count}.{i:0>3}' if i != count else f'_{md5_value}_{count}.zip'
                name = os.path.join(save_dir, f"{file_name_base}.{_suffix}")
                f = open(name, 'wb+')
                if i == 1:
                    f.write(b'\x50\x4b\x07\x08')  # 添加分卷压缩header(4字节)
                    f.write(fp.read(block_size - 4))
                else:
                    f.write(fp.read(block_size))
        fp.close()
        os.remove(save_path)  # 删除临时的 zip 文件
        return save_dir, save_dir_last, count


if __name__ == '__main__':
    file = r"/home/user/P.mp4"  # 原始文件
    path = zip_by_volume(file, ser='linux')
    print(path)  # 输出分卷压缩文件的路径、文件名、文件数
