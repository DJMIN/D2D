import diskcache
import json
import os
import logging
import functools
import asyncio
import shutil

import typing

from d22d.model.midhardware import BaseStore
from d22d.utils.decorators import flyweight

logger = logging.getLogger('diskcachemodel')


class JSONDiskNoC(diskcache.Disk):
    def __init__(self, directory, compress_level=0, **kwargs):
        self.compress_level = compress_level
        super(JSONDiskNoC, self).__init__(directory, **kwargs)

    def put(self, key):
        json_bytes = json.dumps(key).encode('utf-8')
        return super(JSONDiskNoC, self).put(json_bytes)

    def get(self, key, raw):
        data = super(JSONDiskNoC, self).get(key, raw)
        return json.loads(data.decode('utf-8'))

    def store(self, value, read, key=None):
        if not read:
            value = json.dumps(value).encode('utf-8')
        return super(JSONDiskNoC, self).store(value, read)

    def fetch(self, mode, filename, value, read):
        data = super(JSONDiskNoC, self).fetch(mode, filename, value, read)
        if not read:
            data = json.loads(data.decode('utf-8'))
        return data


PATH_REL_DATA = 'data_disk_cache'

PATH_CWD = os.getcwd()
path_abs_base_add = functools.partial(os.path.join, PATH_CWD)
PATH_ABS_DATA = path_abs_base_add(PATH_REL_DATA)
path_abs_data_add = functools.partial(os.path.join, PATH_REL_DATA)
real_path_abs_data_add = functools.partial(os.path.join, PATH_ABS_DATA)


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


# sys.path.append(os.path.join(get_realpath(), './'))


def get_make_local_path(*path):
    local_path = os.path.join(*path)
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


@flyweight
class DiskCache:

    def __init__(self, path=PATH_REL_DATA):
        self.path = path
        self.cache = self.init_cache_default(path).stats()

    @staticmethod
    def init_cache_default(path):
        cache = diskcache.Cache(
            directory=path, disk=JSONDiskNoC, disk_compress_level=6,
            size_limit=int(1024 ** 4),  # 1TB
            cull_limit=0)
        cache.create_tag_index()
        return cache

    def get_cache(self, *args, **kwargs):
        return self.cache.get(*args, **kwargs)

    def set_cache(self, *args, **kwargs):
        with self.cache.transact():
            return self.cache.set(*args, **kwargs)

    def del_cache(self, key, *args, **kwargs):
        if key in self.cache:
            del self.cache[key]

    async def set_cache_async(self, key, value, expire=None, read=False, tag=None, retry=False):
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(None, self.cache.set, key, value, expire, read, tag, retry)
        result = await future
        return result

    def list_cache(self, keys=None, log=True):
        for idx, k in enumerate(self.cache.iterkeys()):
            if keys and k not in keys:
                continue
            else:
                if log:
                    if isinstance(log, int):
                        if not (idx % log):
                            print(f'[cache_print] {k}: {self.cache[k]}')
                    else:
                        print(f'[cache_print] {k}: {self.cache[k]}')
                yield k

    def cull_cache_default(self):
        """
        将缓存目录清理到只剩size_limit的大小
        """
        return self.cache.cull(retry=True)

    def free_self(self, raise_err=False):
        if os.path.exists(self.path):
            can_delete = True
            for root, fs, fns in os.walk(self.path):
                for fn in fns:
                    if not fn.startswith('cache.db'):
                        can_delete = False
                        if raise_err:
                            raise OSError(f"存在非缓存文件{os.path.join(root, fn)}")
                        else:
                            logger.warning(f"存在非缓存文件{os.path.join(root, fn)}")
            if can_delete:
                logger.info(f'删除文件夹：{self.path}:->{os.path.realpath(self.path)}')
                shutil.rmtree(self.path)


class DiskCacheStore(BaseStore):
    def __init__(self, location='/'):
        self.raw_location = location
        self.location = os.path.realpath(location)
        self.client = DiskCache(location)

    def count_data(self, data_type=None, *args, **kwargs):
        return self.client.cache.__len__()

    def list_data(self, data_type=None, *args, **kwargs):
        return self.client.list_cache()

    def check_data(self, position, data_type=None, *args, **kwargs):
        return position in self.client.cache

    def get_data(self, position: typing.Union[str], data_type=None, *args, **kwargs):
        return self.client.get_cache(position)

    def save_data(self, position, data, data_type=None, *args, **kwargs):
        return self.client.set_cache(position, data)

    def delete_data(self, position, data_type=None, *args, **kwargs):
        return self.client.del_cache(position)

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
    a = DiskCache()
    b = DiskCache()
    c = DiskCache(path_abs_data_add("sas"))
    print(id(a))
    print(id(b))
    print(id(c))
    c.free_self()
    a.free_self()
    b.free_self()