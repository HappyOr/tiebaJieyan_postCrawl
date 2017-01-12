#-*-coding=utf-8-*-
""" syslog
Parker中使用自定义facility: local1
"""
# import sys
# import syslog
# import datetime
# import traceback
#
#
# _level_map = {
#     syslog.LOG_ERR: "ERROR",
#     syslog.LOG_INFO: "INFO",
#     syslog.LOG_WARNING: "WARNING",
#     syslog.LOG_DEBUG: "DEBUG",
# }
#
#
# class Logger(object):
#
#     facility = syslog.LOG_LOCAL1
#
#     def __init__(self, name=None):
#         syslog.openlog(facility=self.facility)
#         self.logger_name = name
#
#     def log(self, level, msg):
#         if self.logger_name:
#             msg = "[{name}]{msg}".format(name=self.logger_name, msg=msg)
#         syslog.syslog(level, msg)
#         sys.stdout.write("[{time}-{level}] {msg}\n".format(
#             time=datetime.datetime.now(),
#             level=_level_map.get(level, "UNKNOWN"),
#             msg=msg,
#         ))
#
#     def error(self, msg):
#         self.log(syslog.LOG_ERR, msg)
#
#     def warning(self, msg):
#         self.log(syslog.LOG_WARNING, msg)
#
#     def debug(self, msg):
#         self.log(syslog.LOG_DEBUG, msg)
#
#     def info(self, msg):
#         self.log(syslog.LOG_INFO, msg)
#
#     def exception(self, msg):
#         msg = "{}, {}".format(msg, traceback.format_exc())
#         self.log(syslog.LOG_ERR, msg)
#
#     @staticmethod
#     def debug_mode_log(msg):
#         msg = 'DEBUG_MODE\n{}'.format(msg)
#         Logger('DEBUG_MODE').debug(msg)

import logging


class Logger(object):

    def __init__(self, name,filename):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(filename)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()

        # 定义handler的输出格式formatter
        formatter = logging.Formatter('%(asctime)s-[ %(name)s ]-%(levelname)s: %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def error(self, msg):
        self.logger.error(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def debug(self, msg):
        self.logger.debug(msg)


if __name__ == "__main__":
    logger = Logger("test")

    logger.debug('logger5 debug message')
    #
    logger.info('logger5 info message')
    logger.warning('logger5 warning message')
    logger.error('logger5 error message')
    # logger.critical('logger5 critical message')
