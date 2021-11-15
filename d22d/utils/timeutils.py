import time


def str_to_int_time(string):
    return time.mktime(time.strptime(string, '%Y-%m-%d %H:%M:%S.%f')) - 3600 * 8


def int_to_str_time_gmtime(timeint):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timeint))


def int_to_str_time_localtime(timeint):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timeint))


def int_to_str_time_second(timeint):
    return time.strftime('%H:%M:%S', time.gmtime(timeint / 1000))


def get_time_day():
    return time.strftime("%Y-%m-%d", time.localtime())


def get_time_day_all():
    return time.strftime("%Y_%m_%d__%H_%M_%S", time.localtime())


def custom_time(timestamp):
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y-%m-%d", time_local)
    return dt
