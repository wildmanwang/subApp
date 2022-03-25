# -*- coding:utf-8 -*-
"""
财务处理类
"""
__author__ = "Cliff.wang"
from datetime import datetime, timedelta

from pymysql import Timestamp
from interMysql import MYSQL
import os
import time
import math
import random


class AppFinance():

    def __init__(self, sett, acPara):
        """
        财务类
        sett                配置对象
        acPara{
            "type": 1,      参数类型 1:账户ID 2:实体ID
            "acID": ###,    账户ID
            "enType": #,    实体类型 1:平台 2:服务商 3:商家 4:师傅
            "enID": ###     实体ID
        }
        """
        self.sett = sett
        self.dbFinance = MYSQL(self.sett.dbFinanceHost, self.sett.dbFinancePort, self.sett.dbFinanceUser, self.sett.dbFinancePassword, self.sett.dbFinanceDatabase)
        self.dataInit(acPara)

    def dataInit(self, acPara):
        """
        初始化账户对象
        acPara{
            "type": 1,      参数类型 1:账户ID 2:实体ID
            "acID": ###,    账户ID
            "enType": #,    实体类型 1:平台 2:服务商 3:商家 4:师傅
            "enID": ###     实体ID
        }
        """
        pass

    def fCreateAc(self, data):
        """
        创建账户
        data                账户数据
        """
        pass

    def fGetBancle(self):
        """
        查询余额
        """
        pass

    def fPutin(self, dAmt):
        """
        账户充值
        dAmt                充值金额
        """
        pass

    def fGetout(self, dAmt):
        """
        账户提现
        dAmt                提现金额
        """
        pass

    def fGenerateBill(self, acObj, fund_type, ac_type, busi_type, busi_bill, third_bill, orig_amt, real_amt, busi_summary):
        """
        生成账单
        acObj               对方账户
        fund_type           交易类型 1:常规 2:充值 3:提现
        ac_type             记账类型 1:收款 2:预收 3:信用
        busi_type           业务类型 1:余额操作 2:安装 3:维修
        busi_bill           业务单据号
        third_bill          第三方单据号
        orig_amt            原始交易金额
        real_amt            实际交易金额
        busi_summary        摘要
        """
        pass

    def fFrushBill(self, ac_bill, frush_remark):
        """
        账单冲红
        ac_bill             账单ID
        frush_remark        冲红备注
        """
        pass

    def fGeneratePay(self, ac_bill, paylist, pay_time):
        """
        支付
        ac_bill             账单ID
        paylist             付款列表
            pay_type        付款方式
            pay_amt         付款金额
        pay_time            支付时间 为空时取当前服务器时间
        """
        pass

    def fFrushPay(self, pay_bill, frush_remark):
        """
        支付冲红
        pay_bill            支付ID
        frush_remark        冲红备注
        """
        pass

    def trans_jos_homefw_task(self):
        """
        把京东居家直营店数据批量导入3C
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        # 从京东库导出工单
        rtnData = self._get_jos_homefw_task()
        if rtnData["result"]:
            iNum = rtnData["dataNumber"]
            if iNum > 0:
                rsBill = rtnData["entities"]
                sFile = rtnData["dataString"]
                timeNew = rtnData["dataDatetime"]

                # 工单导入3C
                rtnData = self._put_jdwork_to_3c(sFile)
                if rtnData["result"]:
                    # 删除临时导出的文件
                    os.remove(sFile)

                    # 更新京东数据传输断点
                    rtnData = self._update_jd_breakpoint(timeNew)
                    if rtnData["result"]:
                        rtnData["info"] = "成功导入{num}条京东工单.".format(num=iNum)
                        self.sett.logger.info(rtnData["info"])
            else:
                rtnData["result"] = True
                rtnData["info"] = "没有京东工单需要导入."

        return rtnData


if __name__ == "__main__":
    import os
    from interConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")

    myInter = AppFinance(mySett)
    rtn = myInter.trans_jos_homefw_task()
    print(rtn["info"]) 
