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
    c_sett = None
    c_data = {}

    def __init__(self, sett, init, acPara=None):
        """
        财务类
        sett                配置对象
        init                对象是否初始化 0:不 1:是
        acPara{
            "type": 1,      参数类型 1:账户ID 2:实体ID
            "acID": ###,    账户ID
            "enType": #,    实体类型 1:平台 2:服务商 3:商家 4:师傅
            "enID": ###,    实体ID
            "acType": #     记账类型 1:收款 2:预收 3:信用
        }
        """
        AppFinance.c_sett = sett
        self.acPara = acPara
        self.dbFinance = MYSQL(AppFinance.c_sett.dbFinanceHost, AppFinance.c_sett.dbFinancePort, AppFinance.c_sett.dbFinanceUser, AppFinance.c_sett.dbFinancePassword, AppFinance.c_sett.dbFinanceDatabase)
        self.bInit = False
        rtn = AppFinance.clsInit()
        if init == 1:
            rtn = self.dataInit()

    @classmethod
    def clsInit(cls):
        """
        类初始化
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        bConn = False
        try:
            db = MYSQL(AppFinance.c_sett.dbFinanceHost, AppFinance.c_sett.dbFinancePort, AppFinance.c_sett.dbFinanceUser, AppFinance.c_sett.dbFinancePassword, AppFinance.c_sett.dbFinanceDatabase)
            conn = db.GetConnect()
            bConn = True
            cur = conn.cursor()

            lCol = ["bd_type", "bd_label", "bd_value", "bd_remark"]
            sSql = r"select bd_type, bd_label, bd_value, bd_remark from base_dict order by bd_type asc, bd_value asc"
            cur.execute(sSql)
            rs = cur.fetchall()
            rs = [dict(zip(lCol, line)) for line in rs]
            AppFinance.c_data = {}
            old_type = ""
            old_dict = {}
            for line in rs:
                if line["bd_type"] != old_type:
                    if old_type != "":
                        AppFinance.c_data[old_type] = old_dict
                        old_dict = {}
                old_type = line["bd_type"]
                old_dict[line["bd_value"]] = line["bd_label"]
            if old_type != "":
                AppFinance.c_data[old_type] = old_dict
            rtnData["result"] = True
            rtnData["info"] = "初始化财务数据字典成功."
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def dataInit(self):
        """
        初始化账户对象
        acPara{
            "type": 1,      参数类型 1:账户ID 2:实体ID
            "acID": ###,    账户ID
            "enType": #,    实体类型 1:平台 2:服务商 3:商家 4:师傅
            "enID": ###,    实体ID
            "acType": #     记账类型 1:收款 2:预收 3:信用
        }
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        bConn = False
        try:
            rtn = AppFinance.clsInit()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            lCol = ["id", "name", "simple_name", "entity_type", "entity_id", "ac_type", "ac_balance", "ac_balance_time", "check_flag", "check_balance", "check_time"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select " + sSql[2:len(sSql)] + r" from ac_account where "
            if self.acPara["type"] == 1:
                if self.acPara["acID"] >= 0:
                    sSql += r"id={id}".format(id=self.acPara["acID"])
                else:
                    raise Exception("指定传入账户ID，传值无效.")
            elif self.acPara["type"] == 2:
                if AppFinance.c_data["实体类型"].get(self.acPara["enType"]):
                    if self.acPara["enID"] >= 0:
                        sSql += r"entity_type={entity_type} and entity_id={entity_id}".format(entity_type=self.acPara["enType"], entity_id=self.acPara["enID"])
                    else:
                        raise Exception("指定传入实体ID，传值无效.")
                else:
                    raise Exception("实体类型无效.")
            else:
                raise Exception("传入参数类型只能选择[账号ID,实体ID].")
            if AppFinance.c_data["记账类型"].get(self.acPara["acType"]):
                sSql += r" and ac_type = " + str(self.acPara["acType"])
            else:
                raise Exception("记账类型无效.")
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 1:
                lData = dict(zip(lCol, rs[0]))
                self.bInit = True
            elif len(rs) == 0:
                if self.acPara["type"] == 1:
                    raise Exception("查无此账户：ID={id}，记账类型={acType}".format(
                        id=self.acPara["acID"], 
                        acType=AppFinance.c_data["记账类型"][self.acPara["acType"]])
                    )
                else:
                    raise Exception("查无此账户：{enType}ID={enID}，记账类型={acType}".format(
                        enType=AppFinance.c_data["实体类型"][self.acPara["enType"]],
                        enID=self.acPara["enID"]),
                        acType=AppFinance.c_data["记账类型"][self.acPara["acType"]]
                    )
            else:
                raise Exception("账户异常，找到多个账户数据.")
            self.o_id = lData["id"]
            self.o_name = lData["name"]
            self.o_simple_name = lData["simple_name"]
            self.o_entity_type = lData["entity_type"]
            self.o_entity_id = lData["entity_id"]
            self.o_ac_type = lData["ac_type"]
            self.o_ac_balance = lData["ac_balance"]
            self.o_ac_balance_time = lData["ac_balance_time"]
            self.o_check_flag = lData["check_flag"]
            self.o_check_balance = lData["check_balance"]
            self.o_check_time = lData["check_time"]

            rtnData["result"] = True
            rtnData["info"] = "初始化账户成功."
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def dataRefresh(self):
        """
        属性刷新
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        self.bInit = False
        rtnData = self.dataInit()

        return rtnData

    def fCreateAc(self, data):
        """
        创建账户
        data{                   账户数据
            "name": "XXX",          账户名称
            "simple_name": "XXX",   账户简称
            "entity_type": #,       实体类型 1:平台 2:服务商 3:商家 4:师傅
            "entity_id": #,         实体ID
            "ac_type": #,           记账类型 1:收款 2:预收 3:信用
            "check_flag": #         对账标志 0:不对账 1:对账
        }
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        bConn = False
        try:
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 账户是否已存在
            sSql = r"select count(*) from ac_account where entity_type={entity_type} and entity_id={entity_id} and ac_type={ac_type}".format(
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                ac_type=data["ac_type"]
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if rs[0][0] > 0:
                raise Exception("{name}实体已存在[{ac_type}]型账户，不可重复创建.".format(name=data["name"], ac_type=AppFinance.c_data["记账类型"][data["ac_type"]]))

            # 名称是否被占用（人名除外）
            if data["entity_type"] != 4:
                sSql = r"select count(*) from ac_account where entity_type={entity_type} and entity_id<>{entity_id} and (name='{name}' or simple_name='{simple_name}')".format(
                    entity_type=data["entity_type"],
                    entity_id=data["entity_id"],
                    name=data["name"],
                    simple_name=data["simple_name"]
                )
                cur.execute(sSql)
                rs = cur.fetchall()
                if rs[0][0] > 0:
                    raise Exception("[{name}]或[{simple_name}]名称已被占用，不可使用".format(name=data["name"], simple_name=data["simple_name"]))

            # 账户类型限制
            if data["entity_type"] == 1 and data["ac_type"] == 3 or \
                data["entity_type"] == 3 and data["ac_type"] == 1 or \
                data["entity_type"] == 4 and data["ac_type"] == 2 or \
                data["entity_type"] == 4 and data["ac_type"] == 3 :
                raise Exception(AppFinance.c_data["实体类型"][data["entity_type"]] + "不可创建" + AppFinance.c_data["记账类型"][data["ac_type"]] + "账户.")

            lCol = ["name", "simple_name", "entity_type", "entity_id", "ac_type", "ac_balance", "check_flag", "check_balance"]
            sCol = r""
            for item in lCol:
                sCol += ", " + item
            sSql = r"insert into ac_account(" + sCol[2:len(sCol)] + r") values ( '{name}', '{simple_name}', {entity_type}, {entity_id}, {ac_type}, {ac_balance}, {check_flag}, {check_balance})".format(
                name=data["name"],
                simple_name=data["simple_name"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                ac_type=data["ac_type"],
                ac_balance=0.00,
                check_flag=data["check_flag"],
                check_balance=0.00
            )
            cur.execute(sSql)
            conn.commit()
            rtnData["result"] = True
            self.acPara = {
                "type": 2,
                "acID": 0,
                "enType": data["entity_type"],
                "enID": data["entity_id"],
                "acType": data["ac_type"]
            }
            rtn = self.dataInit()
            if rtn["result"]:
                rtnData["dataNumber"] = self.o_id
                rtnData["info"] = "创建{enType}账号[{id}:{name}]成功.".format(enType=AppFinance.c_data["实体类型"][self.o_entity_type], id=self.o_id, name=self.o_name)
            else:
                rtnData["info"] = "账号初始化失败：" + rtn["info"]
        except Exception as e:
            conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fGetBanlance(self):
        """
        查询余额
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        rtn = self.dataRefresh()
        if rtn["result"] and self.bInit:
            rtnData["result"] = True
            rtnData["dataNumber"] = self.o_ac_balance
            rtnData["datetime"] = self.o_ac_balance_time
            rtnData["info"] = AppFinance.c_data["实体类型"][self.o_entity_type] + "[" + self.o_name + "]的" + AppFinance.c_data["记账类型"][self.o_ac_type] + "账户余额：" + str(self.o_ac_balance)
        else:
            rtnData["info"] = "查询余额失败：" + rtn["info"]

        return rtnData

    def fPutin(self, dAmt):
        """
        账户充值
        dAmt                充值金额
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        bConn = False
        try:
            # 数据初始化
            if not self.bInit:
                rtn = self.dataRefresh()
                if not rtn["result"]:
                    raise Exception("查询余额失败：" + rtn["info"])

            # 账户类型判断
            if self.o_entity_type not in [2, 3]:
                raise Exception(AppFinance.c_data["实体类型"][self.o_entity_type] + "不可充值.")

            # 充值金额判断
            if not dAmt or dAmt <= 0:
                raise Exception("充值金额无效.")
            elif dAmt > 999999.99:
                raise Exception("充值金额太大：{amt}，请检查是否正确".format(amt=dAmt))

            # 充值
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id}".format(
                id=self.o_id,
                amt=dAmt
            )
            cur.execute(sSql)
            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = "充值成功。"
            rtn = self.fGetBanlance()
            if rtn["result"]:
                rtnData["info"] += "账户当前余额：{amt}".format(amt=self.o_ac_balance)
            else:
                rtnData["info"] += rtn["info"]
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

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


if __name__ == "__main__":
    import os
    from appConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")

    myAcc = AppFinance(mySett, 0)
    # rtn = myAcc.fCreateAc({"name": "CSP客户服务平台", "simple_name": "CSP", "entity_type": 1, "entity_id": 1, "ac_type": 1, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "巧匠上门服务有限公司", "simple_name": "巧匠上门", "entity_type": 2, "entity_id": 1, "ac_type": 1, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "鲁班到家家居售后服务平台", "simple_name": "鲁班到家", "entity_type": 2, "entity_id": 2, "ac_type": 1, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "鲁班到家家居售后服务平台", "simple_name": "鲁班到家", "entity_type": 2, "entity_id": 2, "ac_type": 2, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "万师傅家居服务平台", "simple_name": "万师傅", "entity_type": 2, "entity_id": 3, "ac_type": 3, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "石将军家居服务有限公司", "simple_name": "石将军", "entity_type": 3, "entity_id": 1, "ac_type": 2, "check_flag": 0})
    # print(rtn["info"]) 
    # rtn = myAcc.fCreateAc({"name": "华南万达家居服务有限公司", "simple_name": "华南万达", "entity_type": 3, "entity_id": 2, "ac_type": 3, "check_flag": 0})
    # print(rtn["info"]) 
    myAcc = AppFinance(mySett, 1, {"type": 2, "acID": 0, "enType": 2, "enID": 2, "acType": 2})
    rtn = myAcc.fPutin(100)
    print(rtn["info"])
