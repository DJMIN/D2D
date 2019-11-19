# /usr/bin/env python
# coding: utf-8

import logging
import datetime
import time
import os
import threading
import traceback
from enum import Enum


class ELogPriority(Enum):
    zdebug = 1
    zinfo = 2
    zwarn = 3
    zerror = 4
    zfatal = 5


import datetime
import logging
# import tornado.log
import time

try:
    import curses
except ImportError:
    curses = None

from tornado.log import LogFormatter as _LogFormatter


# tornado.log._stderr_supports_color = lambda: True


class LogFormatter(_LogFormatter, object):
    """Init tornado.log.LogFormatter from logging.config.fileConfig"""

    def __init__(self, fmt=None, datefmt=None, color=True, *args, **kwargs):
        if fmt is None:
            # fmt = _LogFormatter.DEFAULT_FORMAT
            fmt = '%(color)s[%(levelname)1.1s][%(asctime)s][%(name)s]' \
                  '[%(processName)s][%(threadName)s]' \
                  '[%(filename)s:%(lineno)d:%(funcName)s()]%(end_color)s' \
                  ' %(message)s'
            # fmt = '[%(asctime)s][%(name)s][%(levelname)s]'\
            #       '[%(processName)s][%(threadName)s]' \
            #       '[%(filename)s:%(lineno)d:%(funcName)s()]: %(message)s'
        super(LogFormatter, self).__init__(color=color, fmt=fmt, datefmt=datefmt, *args, **kwargs)

    def formatTime(self, record, datefmt="%Y-%m-%d %H:%M:%S.%f"):
        """
        Return the creation time of the specified LogRecord as formatted text.
        This method should be called from format() by a formatter which
        wants to make use of a formatted time. This method can be overridden
        in formatters to provide for any specific requirement, but the
        basic behaviour is as follows: if datefmt (a string) is specified,
        it is used with time.strftime() to format the creation time of the
        record. Otherwise, the ISO8601 format is used. The resulting
        string is returned. This function uses a user-configurable function
        to convert the creation time to a tuple. By default, time.localtime()
        is used; to change this for a particular formatter instance, set the
        'converter' attribute to a function with the same signature as
        time.localtime() or time.gmtime(). To change it for all formatters,
        for example if you want all logging times to be shown in GMT,
        set the 'converter' attribute in the Formatter class.
        """
        ct = self.converter(record.created)
        if datefmt:
            # s = time.strftime(datefmt, ct)
            s = str(datetime.datetime.now().strftime(datefmt))
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            s = "%s.%03d" % (t, record.msecs)
            # s = str(datetime.datetime.now().strftime(datefmt))
        return s


class SaveLogHandler(logging.Handler):
    """LogHandler that save records to a list"""

    def __init__(self, saveto=None, *args, **kwargs):
        self.saveto = saveto
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        if self.saveto is not None:
            self.saveto.append(record)

    handle = emit


def enable_pretty_logging(logger=logging.getLogger()):
    channel = logging.StreamHandler()
    channel.setFormatter(LogFormatter())
    logger.addHandler(channel)


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


def get_logger():
    while not os.path.exists('{}/log'.format(os.getcwd())):
        try:
            os.mkdir('{}/log'.format(os.getcwd()))
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            logging.error('{} {}'.format(e, err))
            time.sleep(1)

    from logging.config import fileConfig
    fileConfig('%s/logging.conf' % get_realpath())
    return logging.getLogger(name='mainLogger')


def get_line_num_fast(filename):
    count = 0
    fp = open(filename, "rb")
    while 1:
        buffer = fp.read(1 * 1024 * 1024)
        if not buffer:
            break
        count += buffer.count(b'\n')
    logging.info("文件共统计到%d行" % count)
    fp.close()
    return count


class Logger():
    @classmethod
    def instance(cls):
        logging.info('日志目录：{}/log'.format(os.getcwd()))
        while not os.path.exists('{}/log'.format(os.getcwd())):
            try:
                os.mkdir('{}/log'.format(os.getcwd()))
            except Exception as e:
                err = traceback.format_exc()
                logging.error(e, err)
                logging.error('{} {}'.format(e, err))
                time.sleep(1)

        from logging.config import fileConfig
        import sys
        sys.path.append(get_realpath())
        fileConfig('%s/logging.conf' % get_realpath())
        logger = logging.getLogger(name='mainLogger')

        old_err_func = logger.error

        def _error(msg, ex=None, *args, **kwargs):
            if isinstance(msg, BaseException):
                temp_str = (ex.__str__() + "\n") or ""
                msg_f = f'{temp_str}{msg.__class__} {msg}\n{traceback.format_exc()}'
            elif isinstance(ex, BaseException):
                msg_f = f'{msg}\n{ex.__class__} {ex}\n{traceback.format_exc()}'
            else:
                msg_f = msg
            old_err_func(msg_f, *args, **kwargs)

        if logger.error is not _error:
            logger.error = _error
        return logger


g_log = Logger.instance()

if __name__ == '__main__':
    logz = Logger.instance()
    logz.debug('一个debug信息')
    logz.info('一个info信息')
    logz.warning('一个warning信息')
    try:
        i = []
        i[1]
    except Exception as e:
        logz.error('一个error信息', e)
