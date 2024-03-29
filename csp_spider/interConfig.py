# -*- coding:utf-8 -*-
"""
"""
__author__ = "Cliff.wang"
import configparser
import logging
import os


class Settings():

    def __init__(self, path, file):
        self.path = path
        self.file = file

        self.logger = self._getLogger()

        # config = configparser.ConfigParser()      # 读取特殊字符会报错，例如：%
        self.config = configparser.RawConfigParser()
        self.config.read(os.path.join(path, file), encoding="utf-8")

        # 数据处理端节点
        self.dbHandleHost = self.config.get("handle", "host")
        self.dbHandlePort = self.config.getint("handle", "port")
        self.dbHandleUser = self.config.get("handle", "user")
        self.dbHandlePassword = self.config.get("handle", "password")
        self.dbHandleDatabase = self.config.get("handle", "database")

        # 业务参数
        self.spiderTime = self.config.get("business", "spiderTime")
        self.tmpfilepath = self.config.get("business", "tmpfilepath")

        # 状态
        self.run = True
        self.processing = False

    def _getLogger(self):
        logger = logging.getLogger("[DataInterCatering]")
        handler = logging.FileHandler(os.path.join(self.path, "service.log"))
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        return logger


if __name__ == "__main__":
    i = 1
    i += 1
