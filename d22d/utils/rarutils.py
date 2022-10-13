import logging
import os

from d22d.utils.utils import remove_folder, makedirs


def un_rar(
        file_path, unzip_path='', exclude=None, include=None,
        exclude_path=None, include_path=None, new_file=False, pwd=None):
    from unrar.cffi import rarfile

    """unzip zip file"""
    if exclude is None:
        exclude = []
    if include is None:
        include = []
    if exclude_path is None:
        exclude_path = []
    if include_path is None:
        include_path = []
    rfile = rarfile.RarFile(file_path)
    unzip_path = unzip_path or (file_path + "_files")

    logging.info(f'开始解压文件[{os.path.getsize(file_path) / 1024 / 1024:.3f}MB]：{file_path} ---> {unzip_path}')
    if os.path.exists(unzip_path):
        if new_file:
            remove_folder(unzip_path)
        else:
            raise OSError(f"解压错误：已存在文件：{unzip_path}")
    if os.path.isdir(unzip_path):
        pass
    else:
        os.mkdir(unzip_path)
    namelist = rfile.infolist()
    cnt = 0
    cnt_size = 0
    cnt_unzip = 0
    for idx, f_info in enumerate(namelist):
        cnt = idx
        f_path = f_info.filename
        # f_path_o = f_info.filename
        # f_path = f_path_o.encode('cp437').decode('gb18030')
        root, fn = os.path.split(f_path)
        if include and not any([bool(include_str in fn) for include_str in include]):
            continue
        if exclude and any([bool(exclude_str in fn) for exclude_str in exclude]):
            continue
        if include_path and not any([bool(include_str in f_path) for include_str in include_path]):
            continue
        if exclude_path and any([bool(exclude_str in f_path) for exclude_str in exclude_path]):
            continue

        f_size = f_info.file_size
        cnt_size += f_size

        logging.debug(f'解压到文件 [已处理{cnt}/{len(namelist)} 解压|{cnt_unzip}|个：{cnt_size / 1024 / 1024:.3f}MB]：{f_path} ')
        makedirs(os.path.join(unzip_path, f_path))
        if os.path.isdir(os.path.join(unzip_path, f_path)):
            continue
        with open(os.path.join(unzip_path, f_path), 'wb') as wf:
            wf.write(rfile.read(f_path))

        # zfile.extract(f_path_o, unzip_path, pwd=pwd)
        # makedirs(os.path.join(unzip_path, f_path))
        # shutil.move(os.path.join(unzip_path, f_path_o), os.path.join(unzip_path, f_path))

        cnt_unzip += 1

    logging.info(f'解压结束到文件夹 [{cnt_size / 1024 / 1024:.3f}MB] [已处理{cnt}|解压{cnt_unzip}个]：{unzip_path}')
    return unzip_path

# rar = rarfile.RarFile('360.rar')

# for filename in rar.namelist():
#     info = rar.getinfo(filename)
#     print("Reading {}, {}, {} bytes ({} bytes compressed)".format(info.filename, info.date_time, info.file_size, info.compress_size))
#     data = rar.read(filename)
#     print("\t{}...\n".format(data[:100]))
