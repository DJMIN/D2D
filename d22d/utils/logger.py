# /usr/bin/env python
# coding: utf-8

import logging
import logging.config
import datetime
import time
import sys
import os
import traceback
from enum import Enum
from tornado.log import LogFormatter as _LogFormatter

LOG_PATH = 'log'

def set_log_path(path):
    global LOG_PATH
    LOG_PATH = path


class ELogPriority(Enum):
    zdebug = 1
    zinfo = 2
    zwarn = 3
    zerror = 4
    zfatal = 5


try:
    import curses
except ImportError:
    curses = None

try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
except ImportError:
    from logging.handlers import RotatingFileHandler

    ConcurrentRotatingFileHandler = None


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


def get_logger_by_conf_file():
    while not os.path.exists('{}/log'.format(os.getcwd())):
        try:
            os.mkdir('{}/log'.format(os.getcwd()))
        except Exception as ex:
            import traceback
            err = traceback.format_exc()
            logging.error('{} {}'.format(ex, err))
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


class Logger:
    @classmethod
    def instance(cls):
        logging.info('日志目录：{}/log'.format(os.getcwd()))
        while not os.path.exists('{}/log'.format(os.getcwd())):
            try:
                os.mkdir('{}/log'.format(os.getcwd()))
            except Exception as ex:
                err = traceback.format_exc()
                logging.error(ex, err)
                logging.error('{} {}'.format(ex, err))
                time.sleep(1)

        from logging.config import fileConfig
        import sys
        sys.path.append(get_realpath())
        fileConfig('%s/logging.conf' % get_realpath())
        logger = logging.getLogger(name='mainLogger')

        # old_err_func = logger.error
        #
        # def _error(msg, ex=None, *args, **kwargs):
        #     if isinstance(msg, BaseException):
        #         temp_str = (ex.__str__() + "\n") or ""
        #         msg_f = f'{temp_str}{msg.__class__} {msg}\n{traceback.format_exc()}'
        #     elif isinstance(ex, BaseException):
        #         msg_f = f'{msg}\n{ex.__class__} {ex}\n{traceback.format_exc()}'
        #     else:
        #         msg_f = msg
        #     old_err_func(msg_f, *args, **kwargs)
        #
        # if logger.error is not _error:
        #     logger.error = _error
        return logger


log_conf_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S"
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        "color": {
            "class": "d22d.utils.LogFormatter"
        },
    },
    'handlers': {
        'null': {
            'level': 'DEBUG',
            'class': 'logging.NullHandler',
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'color'
        },
        # 'file_root': {
        #     'level': 'DEBUG',
        #     'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler' if
        #     ConcurrentRotatingFileHandler else 'logging.handlers.RotatingFileHandler',
        #     # 当达到10MB时分割日志
        #     'maxBytes': 1024 * 1024 * 500,
        #     # 最多保留50份文件
        #     'backupCount': 1,
        #     # If delay is true,
        #     # then file opening is deferred until the first call to emit().
        #     'delay': True,
        #     'filename': 'log/root.log',
        #     'formatter': 'color'
        # }
    },
    'loggers': {
        # 'root': {
        #     'handlers': ['file_root', 'console'],
        #     'level': 'DEBUG',
        # },
    }
}


def get_logger(name='mainLogger'):
    # global __g_logger
    # if name in __g_logger:
    #     return __g_logger[name]

    # print(logging.handlers)
    # from logging.config import fileConfig
    # fileConfig('%s/loggingnull.conf' % get_realpath())
    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)
    warn_l = ['WARNING', 'ERROR']
    all_l = ['DEBUG', 'INFO', 'WARNING', 'ERROR']
    for _name, levels in [
        ('requests', warn_l),
        ('urllib3', warn_l),
        ('elasticsearch', warn_l),
        ('sqlalchemy', warn_l),
        ('telethon', warn_l),
        ('kafka', warn_l),
        ('root', all_l),
        (name, all_l),
    ]:
        if _name in log_conf_dict:
            continue
        log_conf_dict['loggers'][_name] = {
            'handlers': ['console'] if name == _name else [],
            'level': levels[0],
        }
        for level in levels:
            log_conf_dict['handlers'][f'file_{_name}_{level}'] = {
                'level': level,
                'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler' if
                ConcurrentRotatingFileHandler else 'logging.handlers.RotatingFileHandler',
                'maxBytes': 1024 * 1024 * 500,  # 当达到500MB时分割日志
                'backupCount': 1,  # 最多保留1份文件
                'delay': True,  # If delay is true, then file opening is deferred until the first call to emit().
                'filename': f'{LOG_PATH}/{_name}_{level.lower()}.log',
                'formatter': 'color'
            }
            log_conf_dict['loggers'][_name]['handlers'].append(f'file_{_name}_{level}')
        log_conf_dict['loggers'][_name]['handlers'] = list(set(log_conf_dict['loggers'][_name]['handlers']))
    logging.config.dictConfig(log_conf_dict)
    __logger = logging.getLogger(name=name)
    # __g_logger[name] = __logger
    # __logger.setLevel(logging.INFO)
    has_stdout = list(filter(lambda x: isinstance(x, logging.StreamHandler), __logger.handlers))
    if not has_stdout:
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt='[%(levelname)s][%(asctime)s.%(msecs)03d]'
                '[%(processName)s:%(threadName)s:%(funcName)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        __logger.addHandler(handler)

    host = os.environ.get('LOGSTASH_HOST')
    port = os.environ.get('LOGSTASH_PORT')
    # host = "192.168.0.73"
    # port = 32647
    from logstash import LogstashHandler

    has_handler = list(filter(lambda x: isinstance(x, LogstashHandler), __logger.handlers))
    if not has_handler:
        if host and port and (isinstance(port, int) or port.isdigit()):
            # from logstash_async.handler import AsynchronousLogstashHandler
            # logstash_handler = AsynchronousLogstashHandler(host, int(port), '')
            from logstash.formatter import LogstashFormatterBase

            def get_extra_fields(_, record):
                fields = {}
                msg = getattr(record, 'msg', None)
                if not isinstance(msg, str):
                    record.msg = str(msg)
                easy_types = (str, bool, dict, float, int, list, type(None))
                for key, value in record.__dict__.items():
                    if isinstance(value, easy_types):
                        fields[key] = value
                    else:
                        fields[key] = repr(value)

                return fields

            LogstashFormatterBase.get_extra_fields = get_extra_fields
            logstash_handler = LogstashHandler(host, int(port), 0)
            __logger.addHandler(logstash_handler)

    return __logger


if __name__ == '__main__':
    logz = Logger.instance()
    logz.debug('一个debug信息')
    logz.info('一个info信息')
    logz.warning('一个warning信息')
    try:
        i = [][1]
    except Exception as exxxx:
        logz.error('一个error信息', exxxx)
