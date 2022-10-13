import pickle
import os
import logging

PICKLE_PATH = 'pkl_cache'

logger = logging.getLogger('pickleutils')


def makedirs(path, check_dot=True):
    if path in ['/', '']:
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


def get_cache_pickle(key, default=None, refresh=False):
    path = f'{PICKLE_PATH}/{key}.pkl'
    res = default
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
