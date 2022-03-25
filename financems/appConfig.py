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

        # 财务数据库链接
        self.dbFinanceHost = self.config.get("finance_srv", "host")
        self.dbFinancePort = self.config.getint("finance_srv", "port")
        self.dbFinanceUser = self.config.get("finance_srv", "user")
        self.dbFinancePassword = self.config.get("finance_srv", "password")
        self.dbFinanceDatabase = self.config.get("finance_srv", "database")

        # 业务参数
        self.tmpFilePath = self.config.get("business", "tmpFilePath")

        # 状态
        self.run = True
        self.processing = False

    def _getLogger(self):
        logger = logging.getLogger("[DataInterCatering]")
        handler = logging.FileHandler(os.path.join(self.path, "service.log"))
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        return logger


if __name__ == "__main__":
    i = 1
    i += 1
