import diskcache
import json
import os
import zlib
import time
import sys
import functools
import asyncio

PATH_REL_DATA = 'data_disk_cache'

PATH_CWD = os.getcwd()
path_abs_base_add = functools.partial(os.path.join, PATH_CWD)
PATH_ABS_DATA = path_abs_base_add(PATH_REL_DATA)
path_abs_data_add = functools.partial(os.path.join, PATH_ABS_DATA)


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


sys.path.append(os.path.join(get_realpath(), './'))


def str_to_int_time(string):
    return time.mktime(time.strptime(string, '%Y-%m-%d %H:%M:%S.%f')) + 3600 * 8


def int_to_str_time_gmtime(timeint):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timeint))


def int_to_str_time_localtime(timeint):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timeint))


def int_to_str_time1(timeint):
    return time.strftime('%H:%M:%S', time.gmtime(timeint / 1000))


def get_time_day():
    return time.strftime("%Y-%m-%d", time.localtime())


def get_time_day_all():
    return time.strftime("%Y_%m_%d__%H_%M_%S", time.localtime())


def get_make_local_path(*path):
    local_path = os.path.join(*path)
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    return local_path


class JSONDisk(diskcache.Disk):
    def __init__(self, directory, compress_level=1, **kwargs):
        self.compress_level = compress_level
        super(JSONDisk, self).__init__(directory, **kwargs)

    def put(self, key):
        json_bytes = json.dumps(key).encode('utf-8')
        data = zlib.compress(json_bytes, self.compress_level)
        return super(JSONDisk, self).put(data)

    def get(self, key, raw):
        data = super(JSONDisk, self).get(key, raw)
        return json.loads(zlib.decompress(data).decode('utf-8'))

    def store(self, value, read, key=None):
        if not read:
            json_bytes = json.dumps(value).encode('utf-8')
            value = zlib.compress(json_bytes, self.compress_level)
        return super(JSONDisk, self).store(value, read)

    def fetch(self, mode, filename, value, read):
        data = super(JSONDisk, self).fetch(mode, filename, value, read)
        if not read:
            data = json.loads(zlib.decompress(data).decode('utf-8'))
        return data


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


cache_default = None


def init_cache_default():
    cache_default = diskcache.Cache(
        directory=PATH_ABS_DATA, disk=JSONDiskNoC, disk_compress_level=6,
        size_limit=int(1024 ** 4), cull_limit=0)
    cache_default.create_tag_index()


def get_cache_default(*args, **kwargs):
    return cache_default.get(*args, **kwargs)


def set_cache_default(*args, **kwargs):
    with cache_default.transact():
        return cache_default.set(*args, **kwargs)


async def set_cache_default_async(key, value, expire=None, read=False, tag=None, retry=False):
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(None, cache_default.set, key, value, expire, read, tag, retry)
    result = await future
    return result


def list_cache_default(keys=None):
    for k in cache_default.iterkeys():
        if keys and k not in keys:
            continue
        else:
            print(f'[cache_print] {k}: {cache_default[k]}')


def cull_cache_default():
    """
    将缓存目录清理到只剩size_limit的大小
    """
    return cache_default.cull(retry=True)


# print(cache_default.size_limit, cache_default.disk_min_file_size, cache_default.cull_limit)
# cache.reset('cull_limit', 0)  # Disable automatic evictions.
if __name__ == '__main__':
    set_cache_default('rwfrwr', 1)
    print(PATH_ABS_DATA, 'tag_index开启' if cache_default.tag_index else 'tag_index尚未开启，请执行,cache_default.create_tag_index()', f'[{cache_default.volume()/1024/1024/1024:.8f}/{cache_default.size_limit/1024/1024/1024:.8f}GB]', cache_default.disk_min_file_size, cache_default.cull_limit)
    list_cache_default()
    # with cache_default.transact():
    #     # cache.set('test', 111)
    #     print(cache_default.get('test'))
        # asyncio.run(set_async('test-key', 'test-value'))
