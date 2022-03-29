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

        # CSP数据节点
        self.dbCSPSrvHost = self.config.get("CSP_srv", "host")
        self.dbCSPSrvPort = self.config.getint("CSP_srv", "port")
        self.dbCSPSrvUser = self.config.get("CSP_srv", "user")
        self.dbCSPSrvPassword = self.config.get("CSP_srv", "password")
        self.dbCSPSrvDatabase = self.config.get("CSP_srv", "database")

        # 京东数据节点
        self.dbJDSrvHost = self.config.get("JD_srv", "host")
        self.dbJDSrvPort = self.config.getint("JD_srv", "port")
        self.dbJDSrvUser = self.config.get("JD_srv", "user")
        self.dbJDSrvPassword = self.config.get("JD_srv", "password")
        self.dbJDSrvDatabase = self.config.get("JD_srv", "database")
        self.cookie = self.config.get("JD_srv", "cookie")

        # 业务参数
        self.tmpFilePath = self.config.get("business", "tmpFilePath")
        self.asoBillInterval = self.config.getint("business", "asoBillInterval")
        self.jdBillInterval = self.config.getint("business", "jdBillInterval")
        self.jdBillBreakpoint = self.config.getfloat("business", "jdBillBreakpoint")
        self.dataAccess = self.config.get("business", "dataAccess")         # request:http请求 db:数据库直连
        if self.dataAccess != "db":
            self.dataAccess = "request"

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
