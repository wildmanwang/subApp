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
            "acType": #     记账类型 1:收款 2:预收 3:预付 4:信用
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

            lCol = ["id", "name", "simple_name", "entity_type", "entity_id", "ac_type", "ac_balance", "ac_balance_time", "credit_line", "check_flag", "check_balance", "check_time"]
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
                if AppFinance.c_data["记账类型"].get(self.acPara["acType"]):
                    sSql += r" and ac_type = " + str(self.acPara["acType"])
                else:
                    raise Exception("记账类型无效.")
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
            self.o_ac_balance = lData["ac_balance"]
            self.o_ac_balance_time = lData["ac_balance_time"]
            self.o_credit_line = lData["credit_line"]
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
            "ac_type": #,           记账类型 1:收款 2:预收 3:预付 4:信用
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
            if data["entity_type"] == 1 and data["ac_type"] == 4 or \
                data["entity_type"] == 2 and data["ac_type"] == 3 or \
                data["entity_type"] == 2 and data["ac_type"] == 4 or \
                data["entity_type"] == 3 and data["ac_type"] == 2 or \
                data["entity_type"] == 4 and data["ac_type"] == 2 or \
                data["entity_type"] == 4 and data["ac_type"] == 3 or \
                data["entity_type"] == 4 and data["ac_type"] == 4 :
                raise Exception(AppFinance.c_data["实体类型"][data["entity_type"]] + "不可创建" + AppFinance.c_data["记账类型"][data["ac_type"]] + "账户.")

            lCol = ["name", "simple_name", "entity_type", "entity_id", "ac_type", "ac_balance", "credit_line", "check_flag", "check_balance"]
            sCol = r""
            for item in lCol:
                sCol += ", " + item
            sSql = r"insert into ac_account(" + sCol[2:len(sCol)] + r") values ( '{name}', '{simple_name}', {entity_type}, {entity_id}, {ac_type}, {ac_balance}, {credit_line}, {check_flag}, {check_balance})".format(
                name=data["name"],
                simple_name=data["simple_name"],
                entity_type=data["entity_type"],
                entity_id=data["entity_id"],
                ac_type=data["ac_type"],
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

    def fGenerateBill(self, acObj, ac_type, busi_type, busi_bill, third_bill, orig_amt, real_amt, busi_summary, acDate=datetime.now()):
        """
        生成账单
        acObj               对方账户
        ac_type             记账类型 1:收款 2:预收 3:预付 4:信用
        busi_type           业务类型 1:充值 2:提现 3:安装 4:维修
        busi_bill           业务单据号
        third_bill          第三方单据号
        orig_amt            原始交易金额
        real_amt            实际交易金额
        busi_summary        摘要
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
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 业务类型判断
            if busi_type == 1:
                # 充值场景
                if self.o_ac_type != 2:
                    raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可充值.")
            elif busi_type == 2:
                # 提现场景
                if self.o_ac_type in [1, 3]:
                    raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可提现.")
            elif busi_type == 3:
                # 还款场景
                if self.o_ac_type != 4:
                    raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可还款.")
            else:
                if not acObj:
                    raise Exception("收款账户无效.")
                else:
                    rtn = acObj.dataRefresh()
                    if not rtn["result"]:
                        raise Exception("获取收款账户信息失败：" + rtn["info"])

            # 金额判断
            if not dAmt or dAmt <= 0:
                raise Exception("交易金额无效.")
            elif dAmt > 999999.99:
                raise Exception("充值金额太大：{amt}，请检查是否正确".format(amt=dAmt))
            if busi_type == 2 and self.o_ac_balance < dAmt:
                raise Exception("提现金额不能大于账户余额.")
            elif busi_type > 3:
                if self.o_ac_type == 4 and self.o_ac_balance + self.o_credit_line < dAmt:
                    raise Exception("账户授信额度{balance}不足.".format(balance=self.o_ac_balance))

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 记录账单明细
            if busi_type == 1 or busi_type == 3:
                sSql = r"insert into ac_bill_flow ( in_ac, in_simple, ac_type, busi_type, busi_bill, third_bill, orig_amt, real_amt, ac_date, busi_summary, frush_flag, check_flag, pay_flag ) " + \
                    r"values ({in_ac}, '{in_simple}', {ac_type}, {busi_type}, {busi_bill}, '{third_bill}', {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, 0, 1) ".format(
                        in_ac=self.o_id,
                        in_simple=self.o_simple_name,
                        ac_type=ac_type,
                        busi_type=busi_type,
                        busi_bill=busi_bill,
                        third_bill=third_bill,
                        orig_amt=dAmt,
                        real_amt=dAmt,
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=busi_summary
                    )
            elif busi_type == 2:
                sSql = r"insert into ac_bill_flow ( out_ac, out_simple, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, check_flag, pay_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, 0, 1) ".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        ac_type=ac_type,
                        busi_type=busi_type,
                        orig_amt=dAmt,
                        real_amt=dAmt,
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=busi_summary
                    )
            else:
                sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, check_flag, pay_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, 0, 1) ".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        in_ac=acObj.o_id,
                        in_simple=acObj.o_simple_name,
                        ac_type=ac_type,
                        busi_type=busi_type,
                        orig_amt=dAmt,
                        real_amt=dAmt,
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=busi_summary
                    )
            cur.execute(sSql)

            # 授信额度更新
            if self.o_ac_type == 4:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))
            if acObj.o_ac_type == 4:
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=acObj.o_id,
                    amt=dAmt,
                    ac_balance=acObj.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=acObj.o_name))

            # 提交事务
            conn.commit()
            rtnData["result"] = True

            rtn = self.fGetBanlance()
            if rtn["result"]:
                rtnData["info"] = busi_summary + "成功."
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

    def fFrushBill(self, ac_bill, frush_remark, acDate=datetime.now()):
        """
        账单冲红
        ac_bill             账单ID
        frush_remark        冲红备注
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
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "check_flag", "pay_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_bill_flow where id={id}".format(
                cols=sSql[2:len(sSql)],
                id=ac_bill
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("账单ID{id}无效.".format(id=ac_bill))
            elif len(rs) == 1:
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=ac_bill, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
                lData = dict(zip(lCol, rs[0]))
            else:
                raise Exception("账单ID{id}出现重复异常.")
            
            # 插入冲红记录
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag, pay_flag ) " + \
                r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, {frush_bill}, '{frush_remark}', 0, 1) ".format(
                    out_ac=lData["out_ac"],
                    out_simple=lData["out_simple"],
                    in_ac=lData["in_ac"],
                    in_simple=lData["in_simple"],
                    ac_type=lData["ac_type"],
                    busi_type=lData["busi_type"],
                    orig_amt=lData["orig_amt"],
                    real_amt= -1 * lData["real_amt"],
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary=busi_summary + "冲红",
                    frush_bill=ac_bill,
                    frush_remark=frush_remark
                )
            cur.execute(sSql)
            billid = conn.insert_id()

            # 授信额度更新
            if self.o_ac_type == 4:
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))
            acObj = AppFinance(AppFinance.c_sett, 1, {"type":1, "acID":lData["in_ac"]})
            if acObj.o_ac_type == 4:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=acObj.o_id,
                    amt=dAmt,
                    ac_balance=acObj.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=acObj.o_name))

            # 提交事务
            conn.commit()
            rtnData["result"] = True

            rtn = self.fGetBanlance()
            if rtn["result"]:
                rtnData["info"] = "账单{id}已冲红.".format(id=ac_bill)
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

    def fGeneratePay(self, ac_bill, paylist, pay_time, acDate=datetime.now()):
        """
        支付
        ac_bill             账单ID
        paylist             付款列表
            pay_type        付款方式
            pay_amt         付款金额
        pay_time            支付时间 为空时取当前服务器时间
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
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "check_flag", "pay_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_bill_flow where id={id}".format(
                cols=sSql[2:len(sSql)],
                id=ac_bill
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("账单ID{id}无效.".format(id=ac_bill))
            elif len(rs) == 1:
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=ac_bill, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
                lData = dict(zip(lCol, rs[0]))
            else:
                raise Exception("账单ID{id}出现重复异常.")
            
            # 收款账号信息
            if lData["in_ac"]:
                acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": lData["in_ac"]})
            
            # 插入支付记录
            if lData["busi_type"] == 1 or lData["busi_type"] == 3:
                sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                    r"values ({in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                        in_ac=self.o_id,
                        in_simple=self.o_simple_name,
                        in_balance=self.o_ac_balance + dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"]
                    )
            elif lData["busi_type"] == 2:
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"]
                    )
            else:
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - dAmt,
                        in_ac=acObj.o_id,
                        in_simple=acObj.o_simple_name,
                        in_balance=acObj.o_ac_balance + dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"]
                    )
            cur.execute(sSql)

            # 余额更新
            if lData["busi_type"] in [1, 3]:
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
            elif lData["busi_type"] == 2:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
            else:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=acObj.o_id,
                    amt=dAmt,
                    ac_balance=acObj.o_ac_balance
                )
            num = cur.execute(sSql)
            if num == 0:
                raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))

            # 提交事务
            conn.commit()
            rtnData["result"] = True

            rtn = self.fGetBanlance()
            if rtn["result"]:
                rtnData["info"] = "账单{id}支付完成.".format(id=ac_bill)
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

    def fFrushPay(self, pay_flow, frush_remark, acDate=datetime.now()):
        """
        支付冲红
        pay_flow            支付ID
        frush_remark        冲红备注
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
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原支付信息
            lCol = ["out_ac", "out_simple", "out_balance", "in_ac", "in_simple", "in_balance", "bill_flow", "busi_type", "pay_type", "pay_amt", "pay_time", "ac_date", "busi_summary", "frush_flag", "check_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_pay_flow where id={id}".format(
                cols=sSql[2:len(sSql)],
                id=pay_flow
            )
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("支付ID{id}无效.".format(id=pay_flow))
            elif len(rs) == 1:
                if lData["out_ac"] != self.o_id:
                    raise Exception("支付[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=pay_flow, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
                lData = dict(zip(lCol, rs[0]))
            else:
                raise Exception("支付ID{id}出现重复异常.")
            
            # 收款账号信息
            if lData["in_ac"]:
                acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": lData["in_ac"]})
            
            # 插入支付记录
            if lData["busi_type"] == 1 or lData["busi_type"] == 3:
                sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                    r"values ({in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                        in_ac=self.o_id,
                        in_simple=self.o_simple_name,
                        in_balance=self.o_ac_balance + dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=-dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"],
                        frush_bill=pay_flow,
                        frush_remark=frush_remark
                    )
            elif lData["busi_type"] == 2:
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=-dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"],
                        frush_bill=pay_flow,
                        frush_remark=frush_remark
                    )
            else:
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - dAmt,
                        in_ac=acObj.o_id,
                        in_simple=acObj.o_simple_name,
                        in_balance=acObj.o_ac_balance + dAmt,
                        bill_flow=billid,
                        busi_type=lData["busi_type"],
                        pay_type=payType,
                        pay_amt=-dAmt,
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary=lData["busi_summary"],
                        frush_bill=pay_flow,
                        frush_remark=frush_remark
                    )
            cur.execute(sSql)

            # 余额更新
            if lData["busi_type"] in [1, 3]:
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
            elif lData["busi_type"] == 2:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
            else:
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=self.o_id,
                    amt=dAmt,
                    ac_balance=self.o_ac_balance
                )
                num = cur.execute(sSql)
                if num == 0:
                    raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                    id=acObj.o_id,
                    amt=dAmt,
                    ac_balance=acObj.o_ac_balance
                )
            num = cur.execute(sSql)
            if num == 0:
                raise Exception("账户{name}数据已变动，更新授信额度失败，请重新操作.".format(name=self.o_name))

            # 提交事务
            conn.commit()
            rtnData["result"] = True

            rtn = self.fGetBanlance()
            if rtn["result"]:
                rtnData["info"] = "支付{id}已冲红.".format(id=pay_flow)
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

    def fPutin(self, payType, dAmt, acDate=datetime.now()):
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
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 账户类型判断
            if self.o_entity_type not in [2, 3]:
                raise Exception(AppFinance.c_data["实体类型"][self.o_entity_type] + "不可充值.")

            # 充值金额判断
            if not dAmt or dAmt <= 0:
                raise Exception("充值金额无效.")
            elif dAmt > 999999.99:
                raise Exception("充值金额太大：{amt}，请检查是否正确".format(amt=dAmt))

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 充值
            sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id} and ac_balance={ac_balance}".format(
                id=self.o_id,
                amt=dAmt,
                ac_balance=self.o_ac_balance
            )
            num = cur.execute(sSql)
            if num == 0:
                raise Exception("账户数据已变动，充值失败，请重新操作.")

            # 记录账单明细
            sSql = r"insert into ac_bill_flow ( in_ac, in_simple, fund_type, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, check_flag, pay_flag ) " + \
                r"values ({in_ac}, '{in_simple}', {fun_type}, {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, 0, 1) ".format(
                    in_ac=self.o_id,
                    in_simple=self.o_simple_name,
                    fun_type=2,
                    ac_type=2,
                    busi_type=1,
                    orig_amt=dAmt,
                    real_amt=dAmt,
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary='预付账户充值'
                )
            cur.execute(sSql)
            billid = conn.insert_id()

            # 记录付款流水
            sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, bill_flow, fun_type, pay_type, pay_amt, pay_time, frush_flag, check_flag ) " + \
                r"values ({in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {fun_type}, '{pay_type}', {pay_amt}, '{pay_time}', 0, 0)".format(
                    in_ac=self.o_id,
                    in_simple=self.o_simple_name,
                    in_balance=self.o_ac_balance + dAmt,
                    bill_flow=billid,
                    fun_type=2,
                    pay_type=payType,
                    pay_amt=dAmt,
                    pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S')
                )
            cur.execute(sSql)

            # 提交事务
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

    def fGetout(self, payType, dAmt, acDate=datetime.now()):
        """
        账户提现
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
            # 数据初始化
            if not self.bInit:
                rtn = self.dataRefresh()
                if not rtn["result"]:
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 账户类型判断
            if self.o_entity_type not in [2]:
                raise Exception(AppFinance.c_data["实体类型"][self.o_entity_type] + "不可提现.")

            # 提现金额判断
            if not dAmt or dAmt <= 0:
                raise Exception("提现金额无效.")
            elif dAmt > 999999.99:
                raise Exception("提现金额太大：{amt}，请检查是否正确".format(amt=dAmt))
            elif dAmt > self.o_ac_balance:
                raise Exception("提现金额不可大于预付余额.")

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 提现
            sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                id=self.o_id,
                amt=dAmt,
                ac_balance=self.o_ac_balance
            )
            num = cur.execute(sSql)
            if num == 0:
                raise Exception("账户数据已变动，提现失败，请重新操作.")

            # 记录账单明细
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, fund_type, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, check_flag, pay_flag ) " + \
                r"values ({out_ac}, '{out_simple}', {fun_type}, {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 0, 0, 1) ".format(
                    out_ac=self.o_id,
                    out_simple=self.o_simple_name,
                    fun_type=3,
                    ac_type=2,
                    busi_type=1,
                    orig_amt=dAmt,
                    real_amt=dAmt,
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary='预付账户提现'
                )
            cur.execute(sSql)
            billid = conn.insert_id()

            # 记录付款流水
            sSql = r"insert into ac_pay_flow ( out_ac, out_simple, in_balance, bill_flow, fun_type, pay_type, pay_amt, pay_time, frush_flag, check_flag ) " + \
                r"values ({out_ac}, '{out_simple}', {out_balance}, {bill_flow}, {fun_type}, '{pay_type}', {pay_amt}, '{pay_time}', 0, 0)".format(
                    out_ac=self.o_id,
                    out_simple=self.o_simple_name,
                    out_balance=self.o_ac_balance - dAmt,
                    bill_flow=billid,
                    fun_type=2,
                    pay_type=payType,
                    pay_amt=dAmt,
                    pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S')
                )
            cur.execute(sSql)

            # 提交事务
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
    myAcc = AppFinance(mySett, 1, {"type": 2, "enType": 2, "enID": 2, "acType": 2})
    rtn = myAcc.fPutin(100)
    print(rtn["info"])
