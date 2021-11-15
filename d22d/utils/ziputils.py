import zip_files.backend
import zip_files.click_extensions
import logging
import os
import pickle
import shutil
import io
import struct

from zipfile import (
    ZipFile, _EndRecData, BadZipFile, _ECD_SIZE,
    _ECD_OFFSET, _ECD_COMMENT, _ECD_LOCATION, _ECD_SIGNATURE,
    stringEndArchive64, sizeEndCentDir64, sizeEndCentDir64Locator, sizeCentralDir,
    structCentralDir, _CD_SIGNATURE, stringCentralDir, _CD_FILENAME_LENGTH,
    ZipInfo, _CD_EXTRA_FIELD_LENGTH, _CD_COMMENT_LENGTH, _CD_LOCAL_HEADER_OFFSET,
    MAX_EXTRACT_VERSION, ZipExtFile, _SharedFile, sizeFileHeader,
    structFileHeader, _FH_SIGNATURE, _FH_FILENAME_LENGTH, _FH_EXTRA_FIELD_LENGTH,
    _FH_GENERAL_PURPOSE_FLAG_BITS, stringFileHeader
)
from pathlib import Path

PICKLE_PATH = 'pkl_cache'

logger = logging.getLogger('ziputils')


def file_exists(path):
    return os.path.exists(path) and os.path.isfile(path)


def makedirs(path, check_dot=True):
    if path in ['/',  '']:
        return
    folder_name = path.split('\\')[-1].split('/')[-1]
    if check_dot:
        is_folder = '.' not in folder_name
        path = os.path.realpath(path)
        out_f_path = os.path.dirname(path)
        if is_folder:
            out_f_path = os.path.join(out_f_path, folder_name)
    else:
        out_f_path = os.path.realpath(path)
    if not os.path.exists(out_f_path):
        logger.info(f'正在创建新文件夹：{out_f_path}，因为{path}需要')
        os.makedirs(out_f_path)


makedirs(PICKLE_PATH)


def get_cache_pickle(key, default=None, refresh=False):
    path = f'{PICKLE_PATH}/{key}.pkl'
    if os.path.exists(path) and not refresh:
        try:
            with open(path, 'rb') as f:
                res = pickle.load(f)
            need_set = False
        except Exception as ex:
            print(f"pickle load: {type(ex)} {ex}")
            need_set = True
    else:
        need_set = True
    if need_set:
        if callable(default):
            default = default()
        set_cache_pickle(key, default)
        res = default
    return res


def set_cache_pickle(key, data):
    makedirs(PICKLE_PATH)
    with open(f'{PICKLE_PATH}/{key}.pkl', 'wb') as f:
        pickle.dump(data, f)


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def zip_folder(f_path, outfile, root_folder=None, auto_root=False, new_file=False):
    """

    :param f_path:
    :param outfile:
    :param root_folder: 创建一个新文件夹作为压缩包内的根目录
    :param auto_root: True 创建一个新文件夹作为作为压缩包内的根目录, 用outfile文件名来命名
    :param new_file: 每次删除老的zip，创建新zip包
    :return:
    """
    logger.info(f'正在压缩zip包{f_path} --> {outfile}')
    makedirs(outfile)

    if new_file:
        remove_file(outfile)

    files = Path(f_path).iterdir()
    if root_folder is None:
        root_folder = Path(f_path).name
        if auto_root:
            root_folder = Path(outfile).stem
    """Compress list of `files`.

    Args:
        root_folder (Path or None): folder name to prepend to `files` inside
            the zip archive
        exclude (list[str]): A list of glob patterns to exclude. Matching is
            done from the right on the path names inside the zip archive.
            Patterns must be relative (not start with a slash)
        exclude_from (list[str]): A list of filenames from which to read
            exclude patterns (one pattern per line)
        exclude_dotfiles (bool): If given as True, exclude all files starting
            with a dot.
        exclude_vcs (bool): If given as True, exclude files and directories
            used by common version control systems (Git, CVS, RCS, SCCS, SVN,
            Arch, Bazaar, Mercurial, and Darcs), e.g.  '.git/', '.gitignore'
            '.gitmodules' '.gitattributes' for Git
        exclude_git_ignores (bool): If given as True, exclude files listed in
            any '.gitignore' in the given `files` or its subfolders.
        outfile (Path): The path of the zip file to be written
        files (Iterable[Path]): The files to include in the zip archive
    """
    zip_files.backend.zip_files(
        debug=True,
        root_folder=root_folder,
        compression=8,
        exclude=[],
        exclude_from=[],
        exclude_dotfiles=False,
        exclude_vcs=False,
        exclude_git_ignores=False,
        outfile=outfile,
        files=files)
    logger.info(f'压缩完成：{f_path} --> {outfile}')


class GBKZipFile(ZipFile):

    def _RealGetContents(self):
        """Read in the table of contents for the ZIP file."""
        fp = self.fp
        try:
            endrec = _EndRecData(fp)
        except OSError:
            raise BadZipFile("File is not a zip file")
        if not endrec:
            raise BadZipFile("File is not a zip file")
        if self.debug > 1:
            print(endrec)
        size_cd = endrec[_ECD_SIZE]  # bytes in central directory
        offset_cd = endrec[_ECD_OFFSET]  # offset of central directory
        self._comment = endrec[_ECD_COMMENT]  # archive comment

        # "concat" is zero, unless zip was concatenated to another file
        concat = endrec[_ECD_LOCATION] - size_cd - offset_cd
        if endrec[_ECD_SIGNATURE] == stringEndArchive64:
            # If Zip64 extension structures are present, account for them
            concat -= (sizeEndCentDir64 + sizeEndCentDir64Locator)

        if self.debug > 2:
            inferred = concat + offset_cd
            print("given, inferred, offset", offset_cd, inferred, concat)
        # self.start_dir:  Position of start of central directory
        self.start_dir = offset_cd + concat
        fp.seek(self.start_dir, 0)
        data = fp.read(size_cd)
        fp = io.BytesIO(data)
        total = 0
        while total < size_cd:
            centdir = fp.read(sizeCentralDir)
            if len(centdir) != sizeCentralDir:
                raise BadZipFile("Truncated central directory")
            centdir = struct.unpack(structCentralDir, centdir)
            if centdir[_CD_SIGNATURE] != stringCentralDir:
                raise BadZipFile("Bad magic number for central directory")
            if self.debug > 2:
                print(centdir)
            filename = fp.read(centdir[_CD_FILENAME_LENGTH])
            flags = centdir[5]
            if flags & 0x800:
                # UTF-8 file names extension
                filename = filename.decode('utf-8')
            else:
                # Historical ZIP filename encoding
                filename = filename.decode('gb18030')
            # Create ZipInfo instance to store file information
            x = ZipInfo(filename)
            x.extra = fp.read(centdir[_CD_EXTRA_FIELD_LENGTH])
            x.comment = fp.read(centdir[_CD_COMMENT_LENGTH])
            x.header_offset = centdir[_CD_LOCAL_HEADER_OFFSET]
            (x.create_version, x.create_system, x.extract_version, x.reserved,
             x.flag_bits, x.compress_type, t, d,
             x.CRC, x.compress_size, x.file_size) = centdir[1:12]
            if x.extract_version > MAX_EXTRACT_VERSION:
                raise NotImplementedError("zip file version %.1f" %
                                          (x.extract_version / 10))
            x.volume, x.internal_attr, x.external_attr = centdir[15:18]
            # Convert date/time code to (year, month, day, hour, min, sec)
            x._raw_time = t
            x.date_time = ((d >> 9) + 1980, (d >> 5) & 0xF, d & 0x1F,
                           t >> 11, (t >> 5) & 0x3F, (t & 0x1F) * 2)

            x._decodeExtra()
            x.header_offset = x.header_offset + concat
            self.filelist.append(x)
            self.NameToInfo[x.filename] = x

            # update total bytes read from central directory
            total = (total + sizeCentralDir + centdir[_CD_FILENAME_LENGTH]
                     + centdir[_CD_EXTRA_FIELD_LENGTH]
                     + centdir[_CD_COMMENT_LENGTH])

            if self.debug > 2:
                print("total", total)

    def open(self, name, mode="r", pwd=None, *, force_zip64=False):
        """Return file-like object for 'name'.

        name is a string for the file name within the ZIP file, or a ZipInfo
        object.

        mode should be 'r' to read a file already in the ZIP file, or 'w' to
        write to a file newly added to the archive.

        pwd is the password to decrypt files (only used for reading).

        When writing, if the file size is not known in advance but may exceed
        2 GiB, pass force_zip64 to use the ZIP64 format, which can handle large
        files.  If the size is known in advance, it is best to pass a ZipInfo
        instance for name, with zinfo.file_size set.
        """
        if mode not in {"r", "w"}:
            raise ValueError('open() requires mode "r" or "w"')
        if pwd and not isinstance(pwd, bytes):
            raise TypeError("pwd: expected bytes, got %s" % type(pwd).__name__)
        if pwd and (mode == "w"):
            raise ValueError("pwd is only supported for reading files")
        if not self.fp:
            raise ValueError(
                "Attempt to use ZIP archive that was already closed")

        # Make sure we have an info object
        if isinstance(name, ZipInfo):
            # 'name' is already an info object
            zinfo = name
        elif mode == 'w':
            zinfo = ZipInfo(name)
            zinfo.compress_type = self.compression
            zinfo._compresslevel = self.compresslevel
        else:
            # Get info object for name
            zinfo = self.getinfo(name)

        if mode == 'w':
            return self._open_to_write(zinfo, force_zip64=force_zip64)

        if self._writing:
            raise ValueError("Can't read from the ZIP file while there "
                             "is an open writing handle on it. "
                             "Close the writing handle before trying to read.")

        # Open for reading:
        self._fileRefCnt += 1
        zef_file = _SharedFile(self.fp, zinfo.header_offset,
                               self._fpclose, self._lock, lambda: self._writing)
        try:
            # Skip the file header:
            fheader = zef_file.read(sizeFileHeader)
            if len(fheader) != sizeFileHeader:
                raise BadZipFile("Truncated file header")
            fheader = struct.unpack(structFileHeader, fheader)
            if fheader[_FH_SIGNATURE] != stringFileHeader:
                raise BadZipFile("Bad magic number for file header")

            fname = zef_file.read(fheader[_FH_FILENAME_LENGTH])
            if fheader[_FH_EXTRA_FIELD_LENGTH]:
                zef_file.read(fheader[_FH_EXTRA_FIELD_LENGTH])
            if zinfo.flag_bits & 0x20:
                # Zip 2.7: compressed patched data
                raise NotImplementedError("compressed patched data (flag bit 5)")

            if zinfo.flag_bits & 0x40:
                # strong encryption
                raise NotImplementedError("strong encryption (flag bit 6)")

            if fheader[_FH_GENERAL_PURPOSE_FLAG_BITS] & 0x800:
                # UTF-8 filename
                fname_str = fname.decode("utf-8")
            else:
                fname_str = fname.decode("gb18030")

            if fname_str != zinfo.orig_filename:
                raise BadZipFile(
                    'File name in directory %r and header %r differ.'
                    % (zinfo.orig_filename, fname))

            # check for encrypted flag & handle password
            is_encrypted = zinfo.flag_bits & 0x1
            if is_encrypted:
                if not pwd:
                    pwd = self.pwd
                if not pwd:
                    raise RuntimeError("File %r is encrypted, password "
                                       "required for extraction" % name)
            else:
                pwd = None
            return ZipExtFile(zef_file, mode, zinfo, pwd, True)
        except Exception as e:
            zef_file.close()
            raise e


def iter_zip_file(
        file_path, unzip_path='', exclude=None, include=None,
        exclude_path=None, include_path=None, new_file=False, pwd=None):
    """unzip zip file"""
    if exclude is None:
        exclude = []
    if include is None:
        include = []
    if exclude_path is None:
        exclude_path = []
    if include_path is None:
        include_path = []
    zfile = GBKZipFile(file_path)
    if pwd:
        if isinstance(pwd, str):
            pwd = pwd.encode()
        zfile.setpassword(pwd)
    unzip_path = unzip_path or (file_path + ".tmp_unzip_files")

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
    namelist = zfile.infolist()
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
        zfile.extract(f_path, unzip_path, pwd=pwd)
        # zfile.extract(f_path_o, unzip_path, pwd=pwd)
        # makedirs(os.path.join(unzip_path, f_path))
        # shutil.move(os.path.join(unzip_path, f_path_o), os.path.join(unzip_path, f_path))

        cnt_unzip += 1
        yield unzip_path, f_path
        os.remove(os.path.join(unzip_path, f_path))
    zfile.close()
    shutil.rmtree(unzip_path)
    logging.info(f'解压结束到文件夹 [{cnt_size / 1024 / 1024:.3f}MB] [已处理{cnt}|解压{cnt_unzip}个]：{unzip_path}')


def un_zip(
        file_path, unzip_path='', exclude=None, include=None,
        exclude_path=None, include_path=None, new_file=False, pwd=None):
    """unzip zip file"""
    if exclude is None:
        exclude = []
    if include is None:
        include = []
    if exclude_path is None:
        exclude_path = []
    if include_path is None:
        include_path = []
    zfile = GBKZipFile(file_path)
    if pwd:
        if isinstance(pwd, str):
            pwd = pwd.encode()
        zfile.setpassword(pwd)
    unzip_path = unzip_path or (file_path + ".tmp_unzip_files")

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
    namelist = zfile.infolist()
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
        zfile.extract(f_path, unzip_path, pwd=pwd)

        # zfile.extract(f_path_o, unzip_path, pwd=pwd)
        # makedirs(os.path.join(unzip_path, f_path))
        # shutil.move(os.path.join(unzip_path, f_path_o), os.path.join(unzip_path, f_path))

        cnt_unzip += 1
    zfile.close()
    logging.info(f'解压结束到文件夹 [{cnt_size / 1024 / 1024:.3f}MB] [已处理{cnt}|解压{cnt_unzip}个]：{unzip_path}')
    return unzip_path


def iter_path(path, exclude=None, include=None, exclude_path=None, include_path=None, return_type=2, open_kwargs=None):
    if exclude is None:
        exclude = []
    if include is None:
        include = []
    if exclude_path is None:
        exclude_path = []
    if include_path is None:
        include_path = []
    if open_kwargs is None:
        open_kwargs = {}
    logging.info(f'开始遍历文件夹：{path}')
    cnt = 0
    cnt_size = 0
    cnt_iter = 0
    for root, fs, fns in os.walk(path):
        for fn in fns:
            cnt += 1
            if include and not any([bool(include_str in fn) for include_str in include]):
                continue
            if exclude and any([bool(exclude_str in fn) for exclude_str in exclude]):
                continue
            f_path = os.path.join(root, fn)
            if include_path and not any([bool(include_str in f_path) for include_str in include_path]):
                continue
            if exclude_path and any([bool(exclude_str in f_path) for exclude_str in exclude_path]):
                continue
            cnt_size += os.path.getsize(f_path)
            cnt_iter += 1
            logging.debug(f'遍历到文件：{f_path} [已处理{cnt_iter}|返回{cnt_iter}个：{cnt_size / 1024 / 1024:.3f}MB]')
            if return_type == 0:
                yield cnt, root, *os.path.splitext(fn)
            elif return_type == 1:
                yield cnt, root, fn
            elif return_type == 2:
                yield cnt, f_path
            elif return_type == 3:
                yield f_path
            elif return_type == 4:
                yield cnt, open(f_path, 'rb', **open_kwargs)
            elif return_type == 5:
                yield open(f_path, 'rb', **open_kwargs)
    logging.info(f'遍历文件结束 [{cnt_size / 1024 / 1024:.3f}MB] [已处理{cnt_iter}|返回{cnt_iter}个]：{path}')


def activate_debug_logger(level=logging.INFO):
    """Global logger used when running from command line."""
    logging.basicConfig(
        format='(%(levelname)s) %(message)s', level=level
    )


if __name__ == '__main__':
    activate_debug_logger()
    # zip_folder('./', './data/tmp.zip', root_folder='data')
    unzpath = un_zip("data.zip", include=['result.log'], new_file=True)
    list(iter_path(unzpath, return_type=3))
