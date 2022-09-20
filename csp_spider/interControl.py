# -*- coding:utf-8 -*-
"""
数据对接控制类
"""
__author__ = "Cliff.wang"
from datetime import datetime, timedelta

from pymysql import Timestamp
from interMysql import MYSQL
import os
import time
import math
import random


class InterControl():

    c_skuMap = {}
    c_skuSub = {}

    def __init__(self, sett):
        """
        接口控制类
        """
        self.sett = sett
        self.dbHandle = MYSQL(self.sett.dbHandleHost, self.sett.dbHandlePort, self.sett.dbHandleUser, self.sett.dbHandlePassword, self.sett.dbHandleDatabase)
        self.dbCSPSrv = MYSQL(self.sett.dbCSPSrvHost, self.sett.dbCSPSrvPort, self.sett.dbCSPSrvUser, self.sett.dbCSPSrvPassword, self.sett.dbCSPSrvDatabase)
        self.dbJDSrv = MYSQL(self.sett.dbJDSrvHost, self.sett.dbJDSrvPort, self.sett.dbJDSrvUser, self.sett.dbJDSrvPassword, self.sett.dbJDSrvDatabase)

        rtn = InterControl.clsInit()
        if rtn["result"]:
            rtn = self.interInit()

    @classmethod
    def clsInit(cls):
        """
        类初始化
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        # 获取sku映射关系
        InterControl.c_skuMap = {}
        InterControl.c_skuMap["100018770219"] = ["ZNS0128", "石将军 R5-N 智能锁（原R5 Link款）"]
        InterControl.c_skuMap["100034250038"] = ["ZNS0122", "石将军 J6 Plus 静谧蓝 智能锁"]
        # InterControl.c_skuMap["69398066937"] = ["ZNS0128", "石将军 R5-N 智能锁(原R5 Link款）"]  # 这个是测试数据
        # InterControl.c_skuMap["10022872192750"] = ["ZNS0128", "石将军 R5-N 智能锁(原R5 Link款）"]  # 这个是测试数据

        # 获取sku商品子项
        InterControl.c_skuSub = {}
        InterControl.c_skuSub["ZNS0128"] = [["ST0020", "石将军 A2 2段标准锁体(方舌 有天地勾)"], ["30004", "石将军-VIP客户-智能锁上门安装服务"]]
        InterControl.c_skuSub["ZNS0122"] = [["ST0020", "石将军 A2 2段标准锁体(方舌 有天地勾)"], ["30004", "石将军-VIP客户-智能锁上门安装服务"]]

        rtnData["result"] = True

        return rtnData

    def interInit(self):
        """
        初始化处理
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        rtnData["result"] = True

        return rtnData

    def spider_run(self):
        """
        跑数据
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        # 获取企微客服统计数据
        rtnData = self.spider_wecom_costomer_service()
        if not rtnData["result"]:
            self.sett.logger.error(rtnData["info"])
        rtnData = self._put_data(rtnData)
        if not rtnData["result"]:
            self.sett.logger.error(rtnData["info"])
        
        # 获取微信公众号统计数据
        rtnData = self.spider_wechat_official_account()
        if not rtnData["result"]:
            self.sett.logger.error(rtnData["info"])
        rtnData = self._put_data(rtnData)
        if not rtnData["result"]:
            self.sett.logger.error(rtnData["info"])

        rtnData["result"] = True

        return rtnData

    def spider_wecom_costomer_service(self):
        """
        获取企业微信客服统计数据
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        rtnData["result"] = True

        return rtnData

    def spider_wechat_official_account(self):
        """
        获取微信公众号统计数据
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        rtnData["result"] = True

        return rtnData

    def _put_data(self, data):
        """
        保存数据
        data: {
            "result": True,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {
                "date/date_staff": [{"key1": "value1", "key2": "value2", "key3": "value3"}, {}, {}]
            }
        }
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        rtnData["result"] = True

        return rtnData


if __name__ == "__main__":
    import os
    from interConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")

    myInter = InterControl(mySett)
    rtn = myInter.trans_jos_homefw_task()
    print(rtn["info"]) 
