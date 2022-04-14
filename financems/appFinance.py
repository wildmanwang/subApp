# -*- coding:utf-8 -*-
"""
财务处理类
"""
__author__ = "Cliff.wang"
from datetime import datetime, timedelta
from pymysql import Timestamp
from interMysql import MYSQL
from decimal import Decimal
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
            "acType": #,     记账类型 1:收付 2:预付 3:信用
            "other_en_type": #, 对方实体类型
            "other_en_id"： #   对方实体ID
        }
        """
        AppFinance.c_sett = sett
        self.acPara = acPara
        self.dbFinance = MYSQL(AppFinance.c_sett.dbFinanceHost, AppFinance.c_sett.dbFinancePort, AppFinance.c_sett.dbFinanceUser, AppFinance.c_sett.dbFinancePassword, AppFinance.c_sett.dbFinanceDatabase)
        self.bInit = False
        rtn = AppFinance.clsInit()
        if init == 1:
            rtn = self.dataInit()
            if not rtn["result"]:
                raise Exception(rtn["info"])

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
            "acType": #,     记账类型 1:收付 2:预付 3:信用
            "other_en_type": #, 对方实体类型
            "other_en_id"： #   对方实体ID
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

            lCol = ["id", "name", "simple_name", "entity_type", "entity_id", "ac_type", "other_en_type", "other_en_id", "ac_balance", "ac_balance_time", "credit_line", "credit_amt", "check_flag", "check_balance", "check_time"]
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
                        sSql += r"entity_type={entity_type} and entity_id={entity_id}".format(
                            entity_type=self.acPara["enType"], 
                            entity_id=self.acPara["enID"]
                        )
                    else:
                        raise Exception("指定传入实体ID，传值无效.")
                else:
                    raise Exception("实体类型无效.")
                if AppFinance.c_data["记账类型"].get(self.acPara["acType"]):
                    sSql += r" and ac_type = " + str(self.acPara["acType"])
                else:
                    raise Exception("记账类型无效.")
                if self.acPara.get("acType") > 1:
                    if not self.acPara.get("other_en_type"):
                        raise Exception("请传入{ac_type}账号对方实体类型.".format(ac_type=AppFinance.c_data["记账类型"][self.acPara.get("ac_type")]))
                    if not self.acPara.get("other_en_id"):
                        raise Exception("请传入{ac_type}账号对方实体ID.".format(ac_type=AppFinance.c_data["记账类型"][self.acPara.get("ac_type")]))
                    sSql += r" and other_en_type={other_en_type} and other_en_id={other_en_id}".format(
                        other_en_type=self.acPara.get("other_en_type"),
                        other_en_id=self.acPara.get("other_en_id")
                    )
            else:
                raise Exception("传入参数类型只能选择[账号ID,实体ID].")
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 1:
                lData = dict(zip(lCol, rs[0]))
                self.bInit = True
            elif len(rs) == 0:
                if self.acPara["type"] == 1:
                    raise Exception("查无此账户：ID={id}".format(
                        id=self.acPara["acID"]
                    ))
                else:
                    raise Exception("查无此账户：{enType}ID={enID}，记账类型={acType}".format(
                        enType=AppFinance.c_data["实体类型"][self.acPara["enType"]],
                        enID=self.acPara["enID"],
                        acType=AppFinance.c_data["记账类型"][self.acPara["acType"]]
                    ))
            else:
                raise Exception("账户异常，找到多个账户数据.")
            self.o_id = lData["id"]
            self.o_name = lData["name"]
            self.o_simple_name = lData["simple_name"]
            self.o_entity_type = lData["entity_type"]
            self.o_entity_id = lData["entity_id"]
            self.o_ac_type = lData["ac_type"]
            self.o_other_en_type = lData["other_en_type"]
            self.o_other_en_id = lData["other_en_id"]
            self.o_ac_balance = lData["ac_balance"]
            self.o_ac_balance_time = lData["ac_balance_time"]
            self.o_credit_line = lData["credit_line"]
            self.o_credit_amt = lData["credit_amt"]
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
            "ac_type": #,           记账类型 1:收付 2:预付 3:信用
            "other_en_type": #,     预付实体类型
            "other_en_id": #,       预付实体ID
            "credit_line": 0.00,    授信额度
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
            iOther_en_type = data.get("other_en_type") if data.get("other_en_type") else 0
            iOther_en_id = data.get("other_en_id") if data.get("other_en_id") else 0
            sSql = r"select count(*) from ac_account where entity_type={entity_type} and entity_id={entity_id} and ac_type={ac_type} and other_en_type={other_en_type} and other_en_id={other_en_id}".format(
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                ac_type=data["ac_type"],
                other_en_type=iOther_en_type,
                other_en_id=iOther_en_id
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
            if data["entity_type"] == 2 and data["ac_type"] == 2 or \
                data["entity_type"] == 4 and data["ac_type"] == 2 or \
                data["entity_type"] == 4 and data["ac_type"] == 3 :
                raise Exception(AppFinance.c_data["实体类型"][data["entity_type"]] + "不可创建" + AppFinance.c_data["记账类型"][data["ac_type"]] + "账户.")

            lCol = ["name", "simple_name", "entity_type", "entity_id", "ac_type", "other_en_type", "other_en_id", "ac_balance", "credit_line", "credit_amt", "check_flag", "check_balance"]
            sCol = r""
            for item in lCol:
                sCol += ", " + item
            sSql = r"insert into ac_account(" + sCol[2:len(sCol)] + r") values ( '{name}', '{simple_name}', {entity_type}, {entity_id}, {ac_type}, {other_en_type}, {other_en_id}, {ac_balance}, {credit_line}, {credit_line}, {check_flag}, {check_balance})".format(
                name=data["name"],
                simple_name=data["simple_name"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                ac_type=data["ac_type"],
                other_en_type=iOther_en_type,
                other_en_id=iOther_en_id,
                ac_balance=0.00,
                credit_line=data["credit_line"],
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
                "acType": data["ac_type"],
                "other_en_type": iOther_en_type,
                "other_en_id": iOther_en_id
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

    def fAcPutin(self, payType, dAmt, payTime=None, acDate=None):
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
            # 参数有效性判断
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 业务类型判断
            if self.o_ac_type != 2:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可充值.")
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            
            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=1, paylist=[{"pay_type": payType, "pay_amt": dAmt}], payTime=payTime, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "充值成功。账户当前余额：{amt}".format(amt=self.o_ac_balance)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fAcPayback(self, payType, dAmt, payTime=None, acDate=None):
        """
        信用账户还款
        dAmt                还款金额
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
            # 参数有效性判断
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 业务类型判断
            if self.o_ac_type != 3:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可还款.")
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            
            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=3, paylist=[{"pay_type": payType, "pay_amt": dAmt}], payTime=payTime, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "还款成功。"

            # 自动支付账单
            rtn = self.fCreditAutoPay()
            rtnData["info"] += rtn["info"] + "账户当前余额：{balance}".format(balance=self.o_ac_balance)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fAcGetback(self, payType, dAmt, payTime=None, acDate=None):
        """
        账户充值提现
        dAmt                提现金额
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
            # 参数有效性判断
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 业务类型判断
            if self.o_ac_type != 2:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可充值提现.")
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提现金额判断
            if dAmt > self.o_ac_balance:
                raise Exception("提现金额不可大于预付余额.")

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True

            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=2, paylist=[{"pay_type": payType, "pay_amt": dAmt}], payTime=payTime, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "充值提现成功。账户当前余额：{amt}".format(amt=self.o_ac_balance)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fAcGetout(self, payType, dAmt, payTime=None, acDate=None):
        """
        账户收款提现
        dAmt                提现金额
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
            # 参数有效性判断
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 业务类型判断
            if self.o_ac_type != 1:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可收款提现.")
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提现金额判断
            if dAmt > self.o_ac_balance:
                raise Exception("提现金额不可大于预付余额.")

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True

            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=2, paylist=[{"pay_type": payType, "pay_amt": dAmt}], payTime=payTime, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "收款提现成功。账户当前余额：{amt}".format(amt=self.o_ac_balance)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fCreditAutoPay(self):
        """
        信用账户自动完成账单付款
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
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 余额检查
            if not self.o_ac_balance > 0:
                raise Exception("余额不足.")
            
            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 信用账户自动完成支付
            acObj = AppFinance(AppFinance.c_sett, 1, {"type": 2, "enType": self.o_other_en_type, "enID": self.o_other_en_id, "acType": 1})
            # 获取支付方式
            iPay = AppFinance.c_sett.payType3
            sSql = r"select name from ac_pay_type where id={id}".format(id=iPay)
            cur.execute(sSql)
            rsPayType = cur.fetchall()
            if len(rsPayType) == 0:
                raise Exception("新增业务失败：获取现金支付方式失败.")
            bContinue = True
            iNum = 0
            dPaid = 0
            while bContinue:
                lCol = ["id", "out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "check_flag", "close_flag"]
                sSql = r""
                for item in lCol:
                    sSql += ", " + item
                sSql = r"select {cols} from ac_bill_flow where out_ac={out_ac} and in_ac={in_ac} and pay_flag=0 and real_amt<={amt} order by id asc".format(
                    cols=sSql[2:len(sSql)],
                    out_ac=self.o_id,
                    in_ac=acObj.o_id,
                    amt=self.o_ac_balance
                )
                cur.execute(sSql)
                rs = cur.fetchall()
                iBill = []
                iPay = []
                if len(rs) > 0:
                    lData = [dict(zip(lCol, line)) for line in rs]
                    for line in lData:
                        if line["real_amt"] > self.o_ac_balance:
                            break
                        rtn = self._generatePayBill(conn, acBill=line["id"], paylist=[{"pay_type":iPay, "pay_amt":line["real_amt"]}])
                        if not rtn["result"]:
                            raise Exception(rtn["info"])
                        iBill.append(line["id"])
                        iPay.extend(rtn["entities"])
                        iNum += 1
                        dPaid += line["real_amt"]
                else:
                    bContinue = False
            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = {"bill": iBill, "pay": iPay}
            rtnData["info"] = "共完成{num}笔合计{amt}账单的支付.".format(num=iNum, amt=dPaid)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()
        
        return rtnData

    def fBusiNew(self, acObj, busi_bill, third_bill, busilist, paylist=None, acDate=None):
        """
        新增业务
        acObj           对方账户
        busi_bill       业务单据号
        third_bill      第三方单据号
        busilist        业务列表
            busi_type       业务类型 4:安装 5:售后
            fee_type        费用类型 401:基础安装费 402:附加费 501:二次上门/空跑费 502:维修费 503:售后退费
            busi_branch     业务门店
            busi_spu        业务SPU
            orig_amt        原始金额
            real_amt        实际金额
        paylist         支付列表
            pay_type        付款方式
            orig_amt        原始金额
            exchange_rate   换算率
            pay_amt         实付金额
            pay_code        支付码
            third_bill      第三方单据号
        acDate          财务日期
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
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])
            rtn = acObj.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 信用账户授信额度判断
            sumBusiRealAmt = 0
            for line in busilist:
                sumBusiRealAmt += line["real_amt"]
            if self.o_ac_type == 3:
                if sumBusiRealAmt > self.o_credit_amt:
                    raise Exception("信用额度[{amt}]不够.".format(amt=self.o_credit_amt))

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 对方账号有效性判断
            if acObj.o_ac_type > 1:
                raise Exception("{ac_type}账户不支持收款.".format(ac_type=AppFinance.c_data["记账类型"][acObj.o_ac_type]))
            
            # 生成账单
            iBill = []
            dPay = {}
            for line in busilist:
                sSummary = "{en_type1}应付{en_type2}[{busi_type}-{fee_type}]".format(
                    en_type1=AppFinance.c_data["实体类型"][self.o_entity_type], 
                    en_type2=AppFinance.c_data["实体类型"][acObj.o_entity_type],
                    busi_type=AppFinance.c_data["业务类型"][line["busi_type"]],
                    fee_type=AppFinance.c_data["费用类型"][line["fee_type"]]
                )
                rtn = self._generateBill(conn, line["busi_type"], line["fee_type"], line["busi_branch"], line["busi_spu"], line["orig_amt"], line["real_amt"], busi_summary=sSummary, acObj=acObj, busi_bill=busi_bill, third_bill=third_bill, acDate=acDate)
                if not rtn["result"]:
                    raise Exception("新增业务失败：" + rtn["info"])
                line["bill_id"] = rtn["dataNumber"]
                dPay[rtn["dataNumber"]] = []
                iBill.append(rtn["dataNumber"])

            iPay = []
            if self.o_ac_type in (1, 2):
                # 支付列表有效性校验
                if not paylist:
                    paylist = []
                
                # 支付金额判断
                paySum = 0
                for line in paylist:
                    paySum += line["pay_amt"]
                    if "orig_amt" not in line:
                        line["orig_amt"] = line["pay_amt"]
                    if "pay_code" not in line:
                        line["pay_code"] = None
                    if "third_bill" not in line:
                        line["third_bill"] = None
                if sumBusiRealAmt > 0 and paySum < sumBusiRealAmt or sumBusiRealAmt < 0 and paySum > sumBusiRealAmt:
                    paylist.insert(0, {
                        "pay_type": AppFinance.c_sett.payType1 if self.o_ac_type == 1 else AppFinance.c_sett.payType2, 
                        "orig_amt":sumBusiRealAmt - paySum,
                        "pay_amt": sumBusiRealAmt - paySum,
                        "pay_code": None,
                        "third_bill": None
                    })
                paySum = 0
                for line in paylist:
                    sSql = r"select name, actual_flag, change_flag, exchange_rate from ac_pay_type where id={id}".format(id=line["pay_type"])
                    cur.execute(sSql)
                    dsPay = cur.fetchall()
                    if len(dsPay) == 0:
                        raise Exception("新增业务失败：获取支付方式[{id}]失败.".format(id=line["pay_type"]))
                    elif len(dsPay) > 1:
                        raise Exception("新增业务失败：获取支付方式[{id}]有多条结果.".format(id=line["pay_type"]))
                    line["name"] = dsPay[0][0]
                    line["actual_flag"] = dsPay[0][1]
                    line["change_flag"] = dsPay[0][2]
                    line["exchange_rate"] = dsPay[0][3]
                # 多余的支付方式
                paylist.sort(key=lambda x:x["pay_amt"], reverse=True)
                paySum = 0
                num = len(paylist)
                for i in range(num):
                    paySum += paylist[i]["pay_amt"]
                    if i < num - 1 and paySum >= sumBusiRealAmt:
                        raise Exception("支付方式[{paytype}]多余.".format(paytype=paylist[num-1]["name"]))
                # 支付顺序：依次按找零标志、实收标志降序排列
                paylist.sort(key=lambda x:x["change_flag"]*10 + x["actual_flag"], reverse=True)
                # 计算实际支付金额：最后一笔支付金额可能大于应付金额，例如票券
                paySum = 0
                num = len(paylist)
                for i in range(num):
                    if i == num - 1:
                        if sumBusiRealAmt - paySum < paylist[i]["pay_amt"]:
                            paylist[i]["pay_amt"] = sumBusiRealAmt - paySum
                    paySum += paylist[i]["pay_amt"]
                # 支付方式均摊到每笔账单
                for payItem in paylist:
                    sumOrig = 0
                    sumPay = 0
                    numBusi = len(busilist)
                    for i in range(numBusi):
                        lsubPay = {}
                        lsubPay["pay_type"] = payItem["pay_type"]
                        lsubPay["exchange_rate"] = payItem["exchange_rate"]
                        lsubPay["pay_code"] = payItem["pay_code"]
                        lsubPay["third_bill"] = payItem["third_bill"]
                        if i == numBusi - 1:
                            subOrig = Decimal(payItem["orig_amt"]) - sumOrig
                            subPay = Decimal(payItem["pay_amt"]) - sumPay
                        else:
                            subOrig = round(Decimal(payItem["orig_amt"] * busilist[i]["real_amt"] / sumBusiRealAmt), 2)
                            subPay = round(Decimal(payItem["pay_amt"] * busilist[i]["real_amt"] / sumBusiRealAmt), 2)
                        lsubPay["orig_amt"] = subOrig
                        lsubPay["pay_amt"] = subPay
                        dPay[busilist[i]["bill_id"]].append(lsubPay)
                        sumOrig += subOrig
                        sumPay += subPay
                # 支付
                for bill in dPay:
                    rtn = self._generatePayBill(conn, acBill=bill, paylist=dPay[bill], acDate=acDate)
                    if not rtn["result"]:
                        raise Exception("新增业务失败：" + rtn["info"])
                    iPay.extend(rtn["entities"])

            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = {"bill": iBill, "pay": iPay}
            rtnData["info"] = "新增业务账款生成成功."
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fBusiFrush(self, busi_bill, frush_remark, acDate=None):
        """
        业务冲红
        busi_bill           业务单据号
        frush_remark        冲红备注
        acDate              财务日期
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
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原账单信息
            lCol = ["id", "frush_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_bill_flow where busi_bill='{busi_bill}'".format(
                cols=sSql[2:len(sSql)],
                busi_bill=busi_bill
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            lData = [dict(zip(lCol, line)) for line in rs]
            if len(lData) == 0:
                raise Exception("单据号[{busi_bill}]无效.".format(busi_bill=busi_bill))
            lData = [line for line in lData if line["frush_flag"] == 0]
            if len(lData) == 0:
                raise Exception("单据[{busi_bill}]的账单已被冲红，不能重复冲红.".format(busi_bill=busi_bill))
            iBill = []
            iPay = []
            for line in lData:
                rtn = self._frushBill(conn, acBill=line["id"], frush_remark=frush_remark, acDate=acDate)
                if not rtn["result"]:
                    raise Exception(rtn["info"])
                iBill.append(rtn["entities"]["bill"])
                iPay.append(rtn["entities"]["pay"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = {"bill": iBill, "pay": iPay}
            rtnData["info"] = "业务单[{busi_bill}]已冲红.".format(busi_bill=busi_bill)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fFrushBill(self, acBill, frush_remark, acDate=None):
        """
        账单冲红
        acBill              账单ID
        frush_remark        冲红备注
        acDate              财务日期
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
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原账单信息
            rtn = self._frushBill(conn, acBill=acBill, frush_remark=frush_remark, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "账单[{id}]已冲红.".format(id=acBill)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fFrushPay(self, pay_flow, frush_remark, pay_code=None, acDate=None):
        """
        支付冲红
        pay_flow            支付ID
        frush_remark        冲红备注
        acDate              财务日期
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
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 支付冲红
            rtn = self._frushPay(conn, None, pay_flow=pay_flow, frush_remark=frush_remark, pay_code=pay_code, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["entities"] = rtn["entities"]
            rtnData["info"] = "支付[{id}]已冲红.".format(id=pay_flow)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def _generateBill(self, conn, busi_type, fee_type, busi_branch, busi_spu, orig_amt, real_amt, busi_summary, acObj=None, busi_bill=None, third_bill=None, acDate=None):
        """
        生成账单
        conn                数据库连接
        busi_type           业务类型 4:安装 5:售后
        fee_type            费用类型 401:基础安装费 402:附加费 501:二次上门/空跑费 502:维修费 503:售后退费
        busi_branch         业务门店
        busi_spu            业务SPU
        orig_amt            原始交易金额
        real_amt            实际交易金额
        busi_summary        摘要
        acObj               对方账户
        busi_bill           业务单据号
        third_bill          第三方单据号
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not AppFinance.c_data["业务类型"].get(busi_type):
                raise Exception("不支持的业务类型[{busi_type}].".format(busi_type=busi_type))
            if not AppFinance.c_data["费用类型"].get(fee_type):
                raise Exception("无效的费用类型[{fee_type}].".format(fee_type=fee_type))
            if not busi_branch:
                raise Exception("请指定业务门店.")
            if not busi_spu:
                raise Exception("请指定业务SPU.")
            if busi_type in (1,2,3):
                raise Exception("账户类操作不需要生成账单.")
            if not real_amt:
                raise Exception("交易金额无效.")
            if real_amt > 999999.99:
                raise Exception("充值金额太大：{amt}，请检查是否正确".format(amt=real_amt))
            if not busi_summary or len(busi_summary) == 0:
                raise Exception("业务摘要无效.")
            if not acObj:
                raise Exception("请指定交易对方账号.")
            if not acDate:
                acDate = datetime.now()

            # 获取游标
            cur = conn.cursor()

            # 记录账单明细
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, fee_type, busi_branch, busi_spu, busi_bill, third_bill, orig_amt, real_amt, ac_date, busi_summary ) " + \
                r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, {fee_type}, {busi_branch}, {busi_spu}, '{busi_bill}', '{third_bill}', {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}' ) ".format(
                    out_ac=self.o_id,
                    out_simple=self.o_simple_name,
                    in_ac=acObj.o_id,
                    in_simple=acObj.o_simple_name,
                    ac_type=self.o_ac_type,
                    busi_type=busi_type,
                    fee_type=fee_type,
                    busi_branch=busi_branch,
                    busi_spu=busi_spu,
                    busi_bill=busi_bill if busi_bill else '',
                    third_bill=third_bill if third_bill else '',
                    orig_amt=orig_amt,
                    real_amt=real_amt,
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary=busi_summary
                )
            cur.execute(sSql)
            iNew = conn.insert_id()
            if not iNew > 0:
                raise Exception("数据插入异常.")
            iFlow = iNew

            # 更新账户
            rtn = self._updateAccount(conn, "bill", busi_type, real_amt, busi_dire=-1, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])
            rtn = acObj._updateAccount(conn, "bill", busi_type, real_amt, busi_dire=1, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 返回数据
            rtnData["result"] = True
            rtnData["dataNumber"] = iFlow
            rtnData["info"] = busi_summary + "成功."
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _generatePayAc(self, conn, busi_type, paylist, payTime=None, acDate=None):
        """
        账户支付
        conn                数据库连接
        busi_type           业务类型 1:充值 2:提现 3:还款
        paylist             付款列表
            pay_type        付款方式
            orig_amt        原始金额
            exchange_rate   换算率
            pay_amt         实付金额
            pay_code        支付码
            third_bill      第三方单据号
        payTime             支付时间 为空时取当前服务器时间
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not AppFinance.c_data["业务类型"].get(busi_type) or busi_type > 3:
                raise Exception("账户支付不支持的业务类型[{busi_type}].".format(busi_type=busi_type)) 
            if busi_type == 1:
                if self.o_ac_type != 2:
                    raise Exception("只有预付账户才支持充值.")
            elif busi_type == 2:
                if self.o_ac_type not in (1, 2):
                    raise Exception("只有收付/预付账户才支持提现.")
            elif busi_type == 3:
                if self.o_ac_type != 3:
                    raise Exception("只有信用账户才支持还款.")
                if len(paylist) > 1:
                    raise Exception("信用还款不支持多方式付款.")
            if len(paylist) == 0:
                raise Exception("没有指定支付类型.")
            paySum = 0
            for line in paylist:
                if line["pay_type"] <= 0:
                    raise Exception("支付类型[{pay_type}]无效.".format(pay_type=line["pay_type"]))
                if not line["pay_amt"] or line["pay_amt"] <= 0:
                    raise Exception("支付金额无效.")
                paySum += line["pay_amt"]
                if "orig_amt" not in line:
                    line["orig_amt"] = line["pay_amt"]
                if "exchange_rate" not in line:
                    line["exchange_rate"] = 1.000000
                if "pay_code" not in line:
                    line["pay_code"] = None
                if "third_bill" not in line:
                    line["third_bill"] = None
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 获取游标
            cur = conn.cursor()

            # 检查账户余额
            if busi_type == 2 and paySum > self.o_ac_balance:
                raise Exception("余额不足.")

            iFlow = []
            for line in paylist:
                # 插入支付记录
                if busi_type == 1 or busi_type == 3:
                    sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, busi_type, bill_flow, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, third_bill, frush_flag, check_flag ) " + \
                        r"values ({in_ac}, '{in_simple}', {in_balance}, {busi_type}, {bill_flow}, {pay_type}, {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, {third_bill}, 0, 0)".format(
                            in_ac=self.o_id,
                            in_simple=self.o_simple_name,
                            in_balance=self.o_ac_balance + line["pay_amt"],
                            busi_type=busi_type,
                            bill_flow="NULL",
                            pay_type=line["pay_type"],
                            orig_amt=line["orig_amt"],
                            exchange_rate=line["exchange_rate"],
                            pay_amt=line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary='充值' if busi_type == 1 else '还款',
                            pay_code="'" + line["pay_code"] + "'" if line["pay_code"] else "NULL",
                            third_bill="'" + line["third_bill"] + "'" if line["third_bill"] else "NULL"
                        )
                elif busi_type == 2:
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, busi_type, bill_flow, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, third_bill, frush_flag, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {busi_type}, {bill_flow}, {pay_type}, {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, {third_bill}, 0, 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance - line["pay_amt"],
                            busi_type=busi_type,
                            bill_flow="NULL",
                            pay_type=line["pay_type"],
                            orig_amt=line["orig_amt"],
                            exchange_rate=line["exchange_rate"],
                            pay_amt=line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary='提现',
                            pay_code="'" + line["pay_code"] + "'" if line["pay_code"] else "NULL",
                            third_bill="'" + line["third_bill"] + "'" if line["third_bill"] else "NULL"
                        )
                cur.execute(sSql)
                iNew = conn.insert_id()
                if not iNew > 0:
                    raise Exception("数据插入异常.")
                iFlow.append(iNew)

            # 更新账户
            rtn = self._updateAccount(conn, "pay", busi_type, busi_amt=paySum, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])
            
            # 返回数据
            rtnData["result"] = True
            rtnData["entities"] = iFlow
            rtnData["info"] = "{ac_type}账户[{name}]{busi_type}成功，当前余额：{balance}.".format(
                ac_type=AppFinance.c_data["记账类型"][self.o_ac_type],
                name=self.o_name,
                busi_type=AppFinance.c_data["业务类型"][busi_type],
                balance=self.o_ac_balance
            )
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _generatePayBill(self, conn, acBill, paylist, payTime=None, acDate=None):
        """
        账单支付
        conn                数据库连接
        paylist             付款列表
            pay_type        付款方式
            orig_amt        原始金额
            exchange_rate   换算率
            pay_amt         实付金额
            pay_code        支付码
            third_bill      第三方单据号
        acBill              账单ID
        payTime             支付时间 为空时取当前服务器时间
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not acBill:
                raise Exception("请指定账单ID.")
            if len(paylist) == 0:
                raise Exception("没有指定支付类型.")
            paySum = 0
            acSum = 0
            iAcPay1 = AppFinance.c_sett.payType1 if self.o_ac_type == 1 else AppFinance.c_sett.payType2 if self.o_ac_type == 2 else AppFinance.c_sett.payType3
            for line in paylist:
                if line["pay_type"] <= 0:
                    raise Exception("支付类型[{pay_type}]无效.".format(pay_type=line["pay_type"]))
                if not line["pay_amt"]:
                    raise Exception("支付金额无效.")
                paySum += line["pay_amt"]
                if line["pay_type"] == iAcPay1:
                    acSum += line["pay_amt"]
                if "orig_amt" not in line:
                    line["orig_amt"] = line["pay_amt"]
                if "exchange_rate" not in line:
                    line["exchange_rate"] = 1.000000
                if "pay_code" not in line:
                    line["pay_code"] = None
                if "third_bill" not in line:
                    line["third_bill"] = None
            if not payTime:
                payTime = datetime.now()
            if not acDate:
                acDate = datetime.now()
            
            # 获取游标
            cur = conn.cursor()
            
            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "fee_type", "busi_bill", "third_bill", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "check_flag", "close_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_bill_flow where id={id}".format(
                cols=sSql[2:len(sSql)],
                id=acBill
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("账单ID{id}无效.".format(id=acBill))
            elif len(rs) == 1:
                lData = dict(zip(lCol, rs[0]))
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=acBill, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
            else:
                raise Exception("账单ID[{id}]出现重复异常.".format(id=acBill))
            if lData["frush_flag"] == 2:
                raise Exception("账单ID[{id}]已被冲红，不可支付.".format(id=acBill))
            if lData["pay_flag"] == 1:
                raise Exception("账单ID[{id}]已支付，不可重复支付.".format(id=acBill))
            elif lData["pay_flag"] == 2:
                raise Exception("账单ID[{id}]无需支付.".format(id=acBill))
            
            # 检查账户余额
            if paySum != lData["real_amt"]:
                raise Exception("支付金额必须等于应付金额.")
            if self.o_ac_type > 1 and acSum > self.o_ac_balance:
                raise Exception("余额不足.")
            
            # 收款账号信息
            if lData["in_ac"]:
                acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": lData["in_ac"]})
                if acObj.o_ac_type > 1:
                    raise Exception("{ac_type}账户不可用于收款.".format(ac_type=AppFinance.c_data["记账类型"][acObj.o_ac_type]))
                iAcPay2 = AppFinance.c_sett.payType1 if acObj.o_ac_type == 1 else AppFinance.c_sett.payType2 if acObj.o_ac_type == 2 else AppFinance.c_sett.payType3
            else:
                raise Exception("请指定收款账户.")

            iFlow = []
            for line in paylist:
                # 插入支付记录
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, bill_flow, busi_type, busi_bill, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, third_bill, frush_flag, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, '{busi_bill}', {pay_type}, {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, {third_bill}, {frush_flag}, 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - line["pay_amt"] if line["pay_type"] == iAcPay1 else self.o_ac_balance,
                        in_ac=acObj.o_id,
                        in_simple=acObj.o_simple_name,
                        in_balance=acObj.o_ac_balance + line["pay_amt"] if line["pay_type"] == iAcPay2 else acObj.o_ac_balance,
                        bill_flow=acBill,
                        busi_type=lData["busi_type"],
                        busi_bill=lData["busi_bill"],
                        pay_type=line["pay_type"],
                        orig_amt=line["orig_amt"],
                        exchange_rate=line["exchange_rate"],
                        pay_amt=line["pay_amt"],
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary="支付：" + lData["busi_summary"],
                        pay_code="'" + line["pay_code"] + "'" if line["pay_code"] else "NULL",
                        third_bill="'" + line["third_bill"] + "'" if line["third_bill"] else "NULL",
                        frush_flag=0 if lData["frush_flag"]==0 else 1
                    )
                cur.execute(sSql)
                iNew = conn.insert_id()
                if not iNew > 0:
                    raise Exception("数据插入异常.")
                iFlow.append(iNew)

                # 更新账户
                rtn = self._updateAccount(conn, "pay", lData["busi_type"], line["pay_amt"], busi_dire=-1, pay_type=line["pay_type"], acDate=acDate)
                if not rtn["result"]:
                    raise Exception(rtn["info"])
                rtn = acObj._updateAccount(conn, "pay", lData["busi_type"], line["pay_amt"], busi_dire=1, pay_type=line["pay_type"], acDate=acDate)
                if not rtn["result"]:
                    raise Exception(rtn["info"])
            
            # 更新原支付记录
            if lData["frush_flag"] == 1:
                sSql = r"update ac_pay_flow set frush_flag=2, bill_remark=if(bill_remark is null, '已被冲红', concat(bill_remark,'；已被冲红')) where bill_flow={bill_flow}".format(
                    bill_flow=acBill
                )
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("账单ID[{bill_flow}]数据异常.".format(bill_flow=acBill))

            # 更新账单支付标志
            sSql = r"update ac_bill_flow set pay_flag=1 where id={id}".format(id=acBill)
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账单[{id}]数据异常.".format(id=acBill))

            # 返回数据
            rtnData["result"] = True
            rtnData["entities"] = iFlow
            rtnData["info"] = "账单{id}支付完成.".format(id=acBill)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _frushBill(self, conn, acBill, frush_remark, acDate=None):
        """
        账单冲红
        conn                数据库连接
        acBill              账单ID
        frush_remark        冲红备注
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()

            # 获取
            cur = conn.cursor()

            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "fee_type", "busi_branch", "busi_spu", "busi_bill", "third_bill", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "check_flag", "close_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_bill_flow where id={id}".format(
                cols=sSql[2:len(sSql)],
                id=acBill
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("账单ID{id}无效.".format(id=acBill))
            elif len(rs) == 1:
                lData = dict(zip(lCol, rs[0]))
                # 账单主体判断
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format( bill=acBill,  entity_type=AppFinance.c_data["实体类型"][self.o_entity_type], name=self.o_name))
                # 已冲红
                if lData["frush_flag"] == 1:
                    raise Exception("账单[{bill}]不能重复冲红.".format( bill=acBill))
                # 支付标志
                if lData["pay_flag"] == 2:
                    raise Exception("账单[{bill}]无需支付，不可冲红.".format(bill=acBill))
            else:
                raise Exception("账单ID{id}出现重复异常.")
            
            # 插入冲红记录
            iBill = []
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, fee_type, busi_branch, busi_spu, busi_bill, third_bill, orig_amt, real_amt, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, pay_flag ) " + \
                r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, {fee_type}, {busi_branch}, {busi_spu}, '{busi_bill}', '{third_bill}', {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', {pay_flag} ) ".format(
                    out_ac=lData["out_ac"],
                    out_simple=lData["out_simple"],
                    in_ac=lData["in_ac"],
                    in_simple=lData["in_simple"],
                    ac_type=lData["ac_type"],
                    busi_type=lData["busi_type"],
                    fee_type=lData["fee_type"],
                    busi_branch=lData["busi_branch"],
                    busi_spu=lData["busi_spu"],
                    busi_bill=lData["busi_bill"],
                    third_bill=lData["third_bill"],
                    orig_amt= -1 * lData["orig_amt"],
                    real_amt= -1 * lData["real_amt"],
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary=lData["busi_summary"],
                    frush_bill=acBill,
                    frush_remark=frush_remark,
                    pay_flag=lData["pay_flag"]
                )
            cur.execute(sSql)
            iNew = conn.insert_id()
            if not iNew > 0:
                raise Exception("数据插入异常.")
            iBill.append(iNew)

            # 更新原单
            sSql = r"update ac_bill_flow set frush_flag=2"
            if lData["pay_flag"] == 0:
                sSql += ", pay_flag=2"
            sSql += ", bill_remark=if(bill_remark is null, '已被冲红', concat(bill_remark,'；已被冲红'))"
            sSql += r" where id={id}".format(id=acBill)
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账单[{id}]数据异常.".format(id=acBill))

            # 支付
            iPay = []
            sSql = r"select id from ac_pay_flow where bill_flow={bill_flow} and frush_flag=0".format(bill_flow=acBill)
            cur.execute(sSql)
            rs = cur.fetchall()
            iPay = []
            if len(rs) > 0:
                rtn = self._frushPay(conn, acBill=acBill, frush_remark=frush_remark, acDate=acDate)
                if not rtn["result"]:
                    raise Exception(rtn["info"])
                iPay.append(rtn["entities"])

            rtnData["result"] = True
            rtnData["entities"] = {"bill": iBill, "pay": iPay}
            rtnData["info"] = "账单{id}已冲红.".format(id=acBill)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _frushPay(self, conn, acBill, frush_remark, pay_flow=None, pay_code=None, acDate=None):
        """
        支付冲红
        conn                数据库连接
        pay_flow            支付ID
        frush_remark        冲红备注
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            if acBill:
                bBillFlag = True
            else:
                bBillFlag = False
                if not pay_flow:
                    raise Exception("请指定冲红的账单ID或支付ID.")

            # 获取游标
            cur = conn.cursor()

            # 获取支付流水及账单信息
            if not bBillFlag:
                sSql = r"select bill_flow, frush_flag from ac_pay_flow where id={id}".format(id=pay_flow)
                cur.execute(sSql)
                rs = cur.fetchall()
                if len(rs) == 0:
                    raise Exception("支付ID{id}无效.".format(id=pay_flow))
                if rs[0][0]:
                    raise Exception("该支付ID{id}存在账单，不可单独冲红.".format(id=pay_flow))
                if rs[0][1] == 1:
                    raise Exception("支付ID{id}是冲红记录，不可被冲红.".format(id=pay_flow))
                elif rs[0][1] == 2:
                    raise Exception("支付ID{id}已被冲红，不可再次冲红.".format(id=pay_flow))
            else:
                sSql = r"select frush_flag, pay_flag, close_flag from ac_bill_flow where id={id}".format(id=acBill)
                cur.execute(sSql)
                rs = cur.fetchall()
                if len(rs) == 0:
                    raise Exception("账单ID[{id}]无效.".format(id=acBill))
                elif len(rs) == 1:
                    lDataBill = dict(zip(["frush_flag", "pay_flag", "close_flag"], rs[0]))
                    # 已冲红
                    if lDataBill["frush_flag"] == 1:
                        raise Exception("账单[{bill}]是冲红记录，不支持支付记录再冲红.".format( bill=acBill))
                    if lDataBill["pay_flag"] == 0:
                        raise Exception("账单[{bill}]未支付，不支持支付记录冲红.".format( bill=acBill))
                    elif lDataBill["pay_flag"] == 2:
                        raise Exception("账单[{bill}]无需支付，不支持支付记录冲红.".format( bill=acBill))
                    if lDataBill["close_flag"] == 1:
                        raise Exception("账单[{bill}]已关账，不支持支付记录冲红.".format( bill=acBill))

            # 获取原支付信息
            lCol = ["id", "out_ac", "out_simple", "out_balance", "in_ac", "in_simple", "in_balance", "busi_type", "busi_bill", "bill_flow", "pay_type", "orig_amt", "exchange_rate", "pay_amt", "pay_time", "ac_date", "busi_summary", "frush_flag", "check_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_pay_flow where ".format(cols=sSql[2:len(sSql)])
            if bBillFlag:
                sSql += "bill_flow={id}".format(id=acBill)
            else:
                sSql += "id={id}".format(id=pay_flow)
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                if bBillFlag:
                    raise Exception("账单ID{id}无效.".format(id=acBill))
                else:
                    raise Exception("支付ID{id}无效.".format(id=pay_flow))
            else:
                lData = [dict(zip(lCol, line)) for line in rs]
                if lData[0]["out_ac"]:
                    if lData[0]["out_ac"] != self.o_id:
                        raise Exception("支付ID[{bill}]不属于{entity_type}账户{name}.".format(
                            bill=pay_flow, 
                            entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                            name=self.o_name
                        ))
                else:
                    if lData[0]["in_ac"] != self.o_id:
                        raise Exception("支付ID[{bill}]不属于{entity_type}账户{name}.".format(
                            bill=pay_flow, 
                            entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                            name=self.o_name
                        ))

            iFlow = []
            for line in lData:
                # 收款账号信息
                if line["out_ac"] and line["in_ac"]:
                    acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": line["in_ac"]})
                
                if line["busi_type"] == 1 or line["busi_type"] == 3:
                    # 插入支付记录
                    sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, busi_type, busi_bill, bill_flow, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({in_ac}, '{in_simple}', {in_balance}, {busi_type}, {busi_bill}, {bill_flow}, '{pay_type}', {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, 1, {frush_bill}, '{frush_remark}', 0)".format(
                            in_ac=self.o_id,
                            in_simple=self.o_simple_name,
                            in_balance=self.o_ac_balance - line["pay_amt"],
                            busi_type=line["busi_type"],
                            busi_bill="'" + line["busi_bill"] + "'" if line["busi_bill"] else "NULL",
                            bill_flow=acBill if bBillFlag else "NULL",
                            pay_type=line["pay_type"],
                            orig_amt=-line["orig_amt"],
                            exchange_rate=line["exchange_rate"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            pay_code="'" + pay_code + "'" if pay_code else "NULL",
                            frush_bill=line["id"],
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iNew = conn.insert_id()
                    if not iNew > 0:
                        raise Exception("数据插入异常.")
                    iFlow.append(iNew)
                    # 更新账户
                    rtn = self._updateAccount(conn, "pay", line["busi_type"], -line["pay_amt"], acDate=acDate)
                    if not rtn["result"]:
                        raise Exception(rtn["info"])
                elif line["busi_type"] == 2:
                    # 插入支付记录
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, busi_type, busi_bill, bill_flow, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {busi_type}, {busi_bill}, {bill_flow}, '{pay_type}', {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, 1, {frush_bill}, '{frush_remark}', 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance + line["pay_amt"],
                            busi_type=line["busi_type"],
                            busi_bill="'" + line["busi_bill"] + "'" if line["busi_bill"] else "NULL",
                            bill_flow=acBill if bBillFlag else "NULL",
                            pay_type=line["pay_type"],
                            orig_amt=-line["orig_amt"],
                            exchange_rate=line["exchange_rate"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            pay_code="'" + pay_code + "'" if pay_code else "NULL",
                            frush_bill=line["id"],
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iNew = conn.insert_id()
                    if not iNew > 0:
                        raise Exception("数据插入异常.")
                    iFlow.append(iNew)
                    # 更新账户
                    rtn = self._updateAccount(conn, "pay", line["busi_type"], -line["pay_amt"], acDate=acDate)
                    if not rtn["result"]:
                        raise Exception(rtn["info"])
                else:
                    # 插入支付记录
                    iAcPay1 = AppFinance.c_sett.payType1 if self.o_ac_type == 1 else AppFinance.c_sett.payType2 if self.o_ac_type == 2 else AppFinance.c_sett.payType3
                    iAcPay2 = AppFinance.c_sett.payType1 if acObj.o_ac_type == 1 else AppFinance.c_sett.payType2 if acObj.o_ac_type == 2 else AppFinance.c_sett.payType3
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, busi_type, busi_bill, bill_flow, pay_type, orig_amt, exchange_rate, pay_amt, pay_time, ac_date, busi_summary, pay_code, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {busi_type}, {busi_bill}, {bill_flow}, '{pay_type}', {orig_amt}, {exchange_rate}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', {pay_code}, 1, {frush_bill}, '{frush_remark}', 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance + line["pay_amt"] if line["pay_type"] == iAcPay1 else self.o_ac_balance,
                            in_ac=acObj.o_id,
                            in_simple=acObj.o_simple_name,
                            in_balance=acObj.o_ac_balance - line["pay_amt"] if line["pay_type"] == iAcPay2 else acObj.o_ac_balance,
                            busi_type=line["busi_type"],
                            busi_bill="'" + line["busi_bill"] + "'" if line["busi_bill"] else "NULL",
                            bill_flow=acBill if bBillFlag else "NULL",
                            pay_type=line["pay_type"],
                            orig_amt=-line["orig_amt"],
                            exchange_rate=line["exchange_rate"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            pay_code="'" + pay_code + "'" if pay_code else "NULL",
                            frush_bill=line["id"],
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iNew = conn.insert_id()
                    if not iNew > 0:
                        raise Exception("数据插入异常.")
                    iFlow.append(iNew)
                    # 更新账户
                    rtn = self._updateAccount(conn, "pay", line["busi_type"], busi_dire=-1, busi_amt=-line["pay_amt"], pay_type=line["pay_type"], acDate=acDate)
                    if not rtn["result"]:
                        raise Exception(rtn["info"])
                    rtn = acObj._updateAccount(conn, "pay", line["busi_type"], busi_dire=1, busi_amt=-line["pay_amt"], pay_type=line["pay_type"], acDate=acDate)
                    if not rtn["result"]:
                        raise Exception(rtn["info"])

                # 更新原单
                sSql = r"update ac_pay_flow set frush_flag=2, bill_remark=if(bill_remark is null, '已被冲红', concat(bill_remark,'；已被冲红')) where id={id}".format(
                    id=line["id"]
                )
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("支付[{id}]数据异常.".format(id=line["id"]))

            # 更新账单支付标志
            if bBillFlag:
                sSql = r"update ac_bill_flow set pay_flag=0 where id={id} and frush_flag=0".format(id=acBill)
                cur.execute(sSql)

            rtnData["result"] = True
            rtnData["entities"] = iFlow
            if bBillFlag:
                rtnData["info"] = "账单[{acBill}]的支付已冲红.".format(acBill=acBill)
            else:
                rtnData["info"] = "支付[{id}]已冲红.".format(id=pay_flow)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _updateAccount(self, conn, ac_oper, busi_type, busi_amt, busi_dire=0, pay_type=None, acDate=None):
        """
        更新账户
        conn                数据库连接
        ac_oper             操作类型 bill:账 pay:支付
        busi_type           业务类型 1:充值 2:提现 3:还款 4:安装 5:售后
        busi_amt            发生金额
        busi_dire           业务方向 -1:付款方 1:收款方 0:账户操作
        pay_type            支付方式
        acDate              财务日期
        """
        rtnData = {
            "result": False,        # 逻辑控制 True/False
            "dataString": "",       # 字符串
            "dataNumber": 0,        # 数字
            "datetime": None,       # 日期时间
            "info": "",             # 信息
            "entities": {}
        }

        try:
            # 参数有效性判断
            if not acDate:
                acDate = datetime.now()
            if busi_type > 3 and ac_oper == "pay" and not pay_type:
                raise Exception("请指定支付方式.")
            
            # 获取游标
            cur = conn.cursor()

            # 生成月汇总记录
            iMonth = acDate.year*100+acDate.month
            sSql = r"select 1 from ac_rpt_month_account where ac_id={ac_id} and month={month}".format(
                ac_id=self.o_id,
                month=iMonth
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                # 查询期初余额
                sSql = "select end_balance from ac_rpt_month_account where ac_id={ac_id} and month=(select max(month) from ac_rpt_month_account where ac_id={ac_id} and month<{month})".format(
                    ac_id=self.o_id,
                    month=iMonth
                )
                cur.execute(sSql)
                rs = cur.fetchall()
                if len(rs) > 0:
                    dBegin = rs[0][0]
                else:
                    dBegin = 0.00
                # 插入初始记录
                sSql = r"insert into ac_rpt_month_account (ac_id, month, begin_balance, ac_in, ac_out, busi_in, busi_out, end_balance) values ({ac_id}, {month}, {balance}, 0, 0, 0, 0, {balance})".format(
                    ac_id=self.o_id,
                    month=iMonth,
                    balance=dBegin
                )
                iNew = cur.execute(sSql)
                if not iNew > 0:
                    raise Exception("新增月初始汇总记录失败.")

            # 更新月汇总表
            sSql = ""
            if busi_type in (1, 3):
                # 充值增加
                sSql = r"update ac_rpt_month_account set ac_in=ac_in+{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
            elif busi_type == 2:
                # 提现增加
                sSql = r"update ac_rpt_month_account set ac_out=ac_out+{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
            else:
                if ac_oper == "bill":
                    if busi_dire == -1:
                        # 支出增加
                        sSql = r"update ac_rpt_month_account set busi_out=busi_out+{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
                    elif busi_dire == 1:
                        # 收入增加
                        sSql = r"update ac_rpt_month_account set busi_in=busi_in+{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
            if len(sSql) > 0:
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("账户[{name}]数据异常.".format(name=self.o_name))

            # 更新账户表、月汇总表
            iAcPay = AppFinance.c_sett.payType1 if self.o_ac_type == 1 else AppFinance.c_sett.payType2 if self.o_ac_type == 2 else AppFinance.c_sett.payType3
            if busi_type in (1,2,3) or ac_oper == "pay" and iAcPay == pay_type:
                sSql1 = ""
                sSql2 = ""
                iOper = 0
                if busi_type in (1, 3) or busi_type > 3 and ac_oper == "pay" and busi_dire == 1:
                    iOper = 1
                    sSql1 = r"update ac_account set ac_balance=ac_balance+{amt}, ac_balance_time=now() where id={ac_id}".format(ac_id=self.o_id, amt=busi_amt)
                    sSql2 = r"update ac_rpt_month_account set end_balance=end_balance+{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
                elif busi_type == 2 or busi_type > 3 and ac_oper == "pay" and busi_dire == -1:
                    iOper = -1
                    sSql1 = r"update ac_account set ac_balance=ac_balance-{amt}, ac_balance_time=now() where id={ac_id}".format(ac_id=self.o_id, amt=busi_amt)
                    sSql2 = r"update ac_rpt_month_account set end_balance=end_balance-{amt} where ac_id={ac_id} and month={month}".format(ac_id=self.o_id, month=iMonth, amt=busi_amt)
                if len(sSql1) > 0:
                    num = cur.execute(sSql1)
                    if num != 1:
                        raise Exception("账户[{name}]数据异常.".format(name=self.o_name))
                    self.o_ac_balance += iOper * busi_amt
                if len(sSql2) > 0:
                    num = cur.execute(sSql2)
                    if num != 1:
                        raise Exception("账户[{name}]数据异常.".format(name=self.o_name))
            
            # 更新信用账户授信额度
            sSql = ""
            iOper = 0
            if self.o_ac_type == 3 and ac_oper == "bill":
                iOper = -1
                sSql = r"update ac_account set credit_amt=credit_amt-{amt} where id={ac_id}".format(ac_id=self.o_id, amt=busi_amt)
            elif self.o_ac_type == 3 and ac_oper == "pay":
                iOper = 1
                sSql = r"update ac_account set credit_amt=credit_amt+{amt} where id={ac_id}".format(ac_id=self.o_id, amt=busi_amt)
            if len(sSql) > 0:
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("账户[{name}]数据异常.".format(name=self.o_name))
                self.o_credit_amt += iOper * busi_amt

            rtnData["result"] = True
            rtnData["info"] = "更新账户[{ac}]成功.".format(ac=self.o_id)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData


if __name__ == "__main__":
    import os
    from appConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")
