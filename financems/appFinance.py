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

            lCol = ["id", "name", "simple_name", "entity_type", "entity_id", "ac_type", "other_en_type", "other_en_id", "ac_balance", "ac_balance_time", "credit_line", "check_flag", "check_balance", "check_time"]
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

            lCol = ["name", "simple_name", "entity_type", "entity_id", "ac_type", "other_en_type", "other_en_id", "ac_balance", "credit_line", "check_flag", "check_balance"]
            sCol = r""
            for item in lCol:
                sCol += ", " + item
            sSql = r"insert into ac_account(" + sCol[2:len(sCol)] + r") values ( '{name}', '{simple_name}', {entity_type}, {entity_id}, {ac_type}, {other_en_type}, {other_en_id}, {ac_balance}, {credit_line}, {check_flag}, {check_balance})".format(
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

    def fAcPutin(self, payType, dAmt, pay_time=datetime.now(), acDate=datetime.now()):
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
            # 业务类型判断
            if self.o_ac_type != 2:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可充值.")

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            
            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=1, paylist=[{"pay_type": payType, "pay_amt": dAmt}], pay_time=pay_time, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

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

    def fAcPayback(self, payType, dAmt, pay_time=datetime.now(), acDate=datetime.now()):
        """
        账户还款
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
            # 业务类型判断
            if self.o_ac_type != 3:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可还款.")

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            
            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=3, paylist=[{"pay_type": payType, "pay_amt": dAmt}], pay_time=pay_time, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = "还款成功。"

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

    def fAcGetback(self, payType, dAmt, pay_time=datetime.now(), acDate=datetime.now()):
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
            # 业务类型判断
            if self.o_ac_type != 2:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可充值提现.")

            # 提现金额判断
            self.dataRefresh()
            if dAmt > self.o_ac_balance:
                raise Exception("提现金额不可大于预付余额.")

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True

            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=2, paylist=[{"pay_type": payType, "pay_amt": dAmt}], pay_time=pay_time, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = "充值提现成功。"

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

    def fAcGetout(self, payType, dAmt, pay_time=datetime.now(), acDate=datetime.now()):
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
            # 业务类型判断
            if self.o_ac_type != 1:
                raise Exception(AppFinance.c_data["记账类型"][self.o_ac_type] + "账户不可收款提现.")

            # 提现金额判断
            self.dataRefresh()
            if dAmt > self.o_ac_balance:
                raise Exception("提现金额不可大于预付余额.")

            # 数据库操作
            conn = self.dbFinance.GetConnect()
            bConn = True

            # 完成支付
            rtn = self._generatePayAc(conn, busi_type=2, paylist=[{"pay_type": payType, "pay_amt": dAmt}], pay_time=pay_time, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = "收款提现成功。"

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

    def fFrushBill(self, ac_bill, frush_remark, acDate=datetime.now()):
        """
        账单冲红
        ac_bill             账单ID
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
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "check_flag", "close_flag"]
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
                lData = dict(zip(lCol, rs[0]))
                # 账单主体判断
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format( bill=ac_bill,  entity_type=AppFinance.c_data["实体类型"][self.o_entity_type], name=self.o_name))
                # 已冲红
                if lData["frush_flag"] == 1:
                    raise Exception("账单[{bill}]不能重复冲红.".format( bill=ac_bill))
                # 支付标志
                if lData["pay_flag"] == 1:
                    raise Exception("账单[{bill}]已支付，不可冲红.".format(bill=ac_bill))
                elif lData["pay_flag"] == 2:
                    raise Exception("账单[{bill}]无需支付，不可冲红.".format(bill=ac_bill))
            else:
                raise Exception("账单ID{id}出现重复异常.")
            
            # 插入冲红记录
            if lData["pay_flag"] == 1:
                iPayFlag = 0
            else:
                iPayFlag = 2
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, orig_amt, real_amt, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, pay_flag ) " + \
                r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', {pay_flag} ) ".format(
                    out_ac=lData["out_ac"],
                    out_simple=lData["out_simple"],
                    in_ac=lData["in_ac"],
                    in_simple=lData["in_simple"],
                    ac_type=lData["ac_type"],
                    busi_type=lData["busi_type"],
                    orig_amt=lData["orig_amt"],
                    real_amt= -1 * lData["real_amt"],
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary=lData["busi_summary"],
                    frush_bill=ac_bill,
                    frush_remark=frush_remark,
                    pay_flag=iPayFlag
                )
            cur.execute(sSql)
            iFlow = conn.insert_id()

            # 更新原单
            sSql = r"update ac_bill_flow set frush_flag=2"
            if lData["pay_flag"] == 0:
                sSql += ", pay_flag=2"
            sSql += ", bill_remark=if(bill_remark is null, '已被冲红', concat(bill_remark,'；已被冲红'))"
            sSql += r" where id={id}".format(id=ac_bill)
            cur.execute(sSql)

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["dataNumber"] = iFlow
            rtnData["info"] = "账单{id}已冲红.".format(id=ac_bill)
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
            # 数据初始化
            if not self.bInit:
                rtn = self.dataRefresh()
                if not rtn["result"]:
                    raise Exception("查询账户数据失败：" + rtn["info"])

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取账单信息
            sSql = r"select bill_flow from ac_pay_flow where id={id}".format(id=pay_flow)
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                raise Exception("支付ID{id}无效.".format(id=pay_flow))
            if rs[0][0]:
                bBillFlag = True
                iBill = rs[0][0]
                sSql = r"select frush_flag, pay_flag, close_flag from ac_bill_flow where id={id}".format(id=iBill)
                cur.execute(sSql)
                rs = cur.fetchall()
                if len(rs) == 0:
                    raise Exception("账单ID{id}无效.".format(id=iBill))
                elif len(rs) == 1:
                    lDataBill = dict(zip(["frush_flag", "pay_flag", "close_flag"], rs[0]))
                    # 已冲红
                    if lDataBill["frush_flag"] > 0:
                        raise Exception("账单[{bill}]是冲红记录/或已被冲红，不支持支付记录再冲红.".format( bill=iBill))
                    if lDataBill["pay_flag"] == 0:
                        raise Exception("账单[{bill}]未支付，不支持支付记录冲红.".format( bill=iBill))
                    elif lDataBill["pay_flag"] == 2:
                        raise Exception("账单[{bill}]无需支付，不支持支付记录冲红.".format( bill=iBill))
                    if lDataBill["close_flag"] == 1:
                        raise Exception("账单[{bill}]已关账，不支持支付记录冲红.".format( bill=iBill))
            else:
                bBillFlag = False

            # 获取原支付信息
            lCol = ["out_ac", "out_simple", "out_balance", "in_ac", "in_simple", "in_balance", "busi_type", "bill_flow", "pay_type", "pay_amt", "pay_time", "ac_date", "busi_summary", "frush_flag", "check_flag"]
            sSql = r""
            for item in lCol:
                sSql += ", " + item
            sSql = r"select {cols} from ac_pay_flow where ".format(cols=sSql[2:len(sSql)])
            if bBillFlag:
                sSql += "bill_flow={id}".format(id=iBill)
            else:
                sSql += "id={id}".format(id=pay_flow)
            cur.execute(sSql)
            rs = cur.fetchall()
            if len(rs) == 0:
                if bBillFlag:
                    raise Exception("账单ID{id}无效.".format(id=iBill))
                else:
                    raise Exception("支付ID{id}无效.".format(id=pay_flow))
            else:
                lData = [dict(zip(lCol, line)) for line in rs]
                if lData[0]["out_ac"] and lData[0]["in_ac"] and lData[0]["out_ac"] != self.o_id:
                    raise Exception("支付ID[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=pay_flow, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
                elif lData[0]["out_ac"] and lData[0]["out_ac"] != self.o_id or lData[0]["in_ac"] and lData[0]["in_ac"] != self.o_id:
                    raise Exception("支付ID[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=pay_flow, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))

            for line in lData:
                # 收款账号信息
                if line["out_ac"] and line["in_ac"]:
                    acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": line["in_ac"]})
                
                if line["busi_type"] == 1 or line["busi_type"] == 3:
                    # 插入支付记录
                    sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, busi_type, bill_flow, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({in_ac}, '{in_simple}', {in_balance}, {busi_type}, {bill_flow}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                            in_ac=self.o_id,
                            in_simple=self.o_simple_name,
                            in_balance=self.o_ac_balance - line["pay_amt"],
                            bill_flow=iBill if bBillFlag else "NULL",
                            busi_type=line["busi_type"],
                            pay_type=line["pay_type"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            frush_bill=pay_flow,
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iFlow = conn.insert_id()
                    # 更新余额
                    sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id}".format(
                        id=self.o_id,
                        amt=line["pay_amt"]
                    )
                    num = cur.execute(sSql)
                    if num != 1:
                        raise Exception("账户{name}数据异常.".format(name=self.o_name))
                elif line["busi_type"] == 2:
                    # 插入支付记录
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, busi_type, bill_flow, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {busi_type}, {bill_flow}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance + line["pay_amt"],
                            busi_type=line["busi_type"],
                            bill_flow=iBill if bBillFlag else "NULL",
                            pay_type=line["pay_type"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            frush_bill=pay_flow,
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iFlow = conn.insert_id()
                    # 更新余额
                    sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id}".format(
                        id=self.o_id,
                        amt=line["pay_amt"]
                    )
                    num = cur.execute(sSql)
                    if num != 1:
                        raise Exception("账户{name}数据异常.".format(name=self.o_name))
                else:
                    # 插入支付记录
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, busi_type, bill_flow, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, frush_bill, frush_remark, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {busi_type}, {bill_flow}, '{pay_type}', {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 1, {frush_bill}, '{frush_remark}', 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance + line["pay_amt"],
                            in_ac=acObj.o_id,
                            in_simple=acObj.o_simple_name,
                            in_balance=acObj.o_ac_balance - line["pay_amt"],
                            busi_type=line["busi_type"],
                            bill_flow=iBill if bBillFlag else "NULL",
                            pay_type=line["pay_type"],
                            pay_amt=-line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary=line["busi_summary"],
                            frush_bill=pay_flow,
                            frush_remark=frush_remark
                        )
                    cur.execute(sSql)
                    iFlow = conn.insert_id()
                    # 更新余额
                    sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id}".format(
                        id=self.o_id,
                        amt=line["pay_amt"]
                    )
                    num = cur.execute(sSql)
                    if num != 1:
                        raise Exception("账户{name}数据异常.".format(name=self.o_name))
                    sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id} and ac_balance={ac_balance}".format(
                        id=acObj.o_id,
                        amt=line["pay_amt"],
                        ac_balance=acObj.o_ac_balance
                    )
                    num = cur.execute(sSql)
                    if num != 1:
                        raise Exception("账户{name}数据异常.".format(name=self.o_name))

                # 更新原单
                sSql = r"update ac_pay_flow set frush_flag=2, bill_remark=if(bill_remark is null, '已被冲红', concat(bill_remark,'；已被冲红')) where id={id}".format(
                    id=pay_flow
                )

            # 更新账单支付标志
            if bBillFlag:
                sSql = r"update ac_bill_flow set pay_flag=0 where id={id}".format(id=iBill)
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("账户{name}数据异常.".format(name=self.o_name))

            # 提交事务
            conn.commit()
            rtnData["result"] = True
            rtnData["dataNumber"] = iFlow
            rtnData["info"] = "支付{id}已冲红.".format(id=pay_flow)
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fBusiNew(self, orig_amt, real_amt, acObj, busi_type, busi_bill, third_bill=None, acDate=datetime.now()):
        """
        新增业务
        orig_amt        原始金额
        real_amt        实际金额
        acObj           对方账户
        busi_type       业务类型 4:安装 5:售后
        busi_bill       业务单据号
        third_bill      对方单据号
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
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception("查询账户数据失败：" + rtn["info"])
            rtn = acObj.dataRefresh()
            if not rtn["result"]:
                raise Exception("查询对方账户数据失败：" + rtn["info"])

            # 信用账户授信额度判断
            if self.o_ac_type == 3:
                if real_amt > self.o_credit_amt:
                    raise Exception("信用额度[{amt}]不够.".format(amt=self.o_credit_amt))

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 对方账号有效性判断
            if acObj.o_ac_type > 1:
                raise Exception("{ac_type}账户不支持收款.".format(ac_type=AppFinance.c_data["记账类型"][acObj.o_ac_type]))
            
            # 生成账单
            sSummary = "{en_type1}应付{en_type2}佣金".format(
                en_type1=AppFinance.c_data["实体类型"][self.o_entity_type], 
                en_type2=AppFinance.c_data["实体类型"][acObj.o_entity_type]
            )
            rtn = self._generateBill(conn, busi_type, orig_amt, real_amt, busi_summary=sSummary, acObj=acObj, busi_bill=busi_bill, third_bill=third_bill, acDate=acDate)
            if not rtn["result"]:
                raise Exception("新增业务失败：" + rtn["info"])
            iBill = rtn["dataNumber"]

            if self.o_ac_type in (1, 2):
                # 支付
                sSql = r"select id from ac_pay_type where pay_mode=1 and actual_flag=1"
                cur.execute(sSql)
                rsPayType = cur.fetchall()
                if len(rsPayType) == 0:
                    raise Exception("新增业务失败：获取现金支付方式失败.")
                rtn = self._generatePayBill(conn, busi_type, [{"pay_type": rsPayType[0][0], "pay_amt": real_amt}], ac_bill=iBill, acDate=acDate)
                if not rtn["result"]:
                    raise Exception("新增业务失败：" + rtn["info"])

            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = "新增业务账款生成成功."
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fBusiAdjust(self, ac_bill, orig_amt, real_amt, adj_remark, third_bill=None, acDate=datetime.now()):
        """
        业务账款调整
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
                raise Exception("查询账户数据失败：" + rtn["info"])

            # 信用账户授信额度判断
            if self.o_ac_type == 3:
                if real_amt > self.o_credit_amt:
                    raise Exception("信用额度[{amt}]不够.".format(amt=self.o_credit_amt))

            # 数据库连接
            conn = self.dbFinance.GetConnect()
            bConn = True
            cur = conn.cursor()

            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "busi_bill", "third_bill", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "close_flag"]
            sCols = ""
            for item in lCol:
                sCols += ", " + item
            sCols = sCols[2:len(sCols)]
            sSql = r"select {sCols} from ac_bill_flow where id={ac_bill}".format(sCols=sCols, ac_bill=ac_bill)
            cur.execute(sSql)
            rsBill = cur.fetchall()
            if len(rsBill) == 0:
                raise Exception("账单ID[{ac_bill}]无效.".format(ac_bill=ac_bill))
            rdBill = dict(zip(lCol, rsBill[0]))
            if rdBill["out_ac"] != self.o_id:
                raise Exception("账单[{ac_bill}]不属于{en_type}{en_name}.".format(
                    ac_bill=ac_bill, 
                    en_type=AppFinance.c_data["实体类型"][self.o_entity_type]),
                    en_name=self.o_simple_name
                )
            if rdBill["frush_flag"] == 1:
                raise Exception("账单[{ac_bill}]是冲红单，不可调账.".format(ac_bill=ac_bill))
            elif rdBill["frush_flag"] == 2:
                raise Exception("账单[{ac_bill}]已被冲红，不可调账.".format(ac_bill=ac_bill))
            if rdBill["close_flag"] == 1:
                raise Exception("账单[{ac_bill}]已关账，不可调账.".format(ac_bill=ac_bill))
            
            # 商家应付平台佣金调整金额
            acObj = AppFinance(self.c_sett, 1, {"type": 1, "acID": rdBill["in_ac"]})
            rtn = self._generateBill(conn, rdBill["busi_type"], orig_amt, real_amt, rdBill["busi_summary"] + "：" + adj_remark, acObj, rdBill["busi_bill"], (third_bill if third_bill else rdBill["third_bill"]), acDate)
            if not rtn["result"]:
                raise Exception("生成调整账单失败：" + rtn["info"])
            iBill = rtn["dataNumber"]

            if self.o_ac_type in (1, 2):
                # 支付
                sSql = r"select id from ac_pay_type where pay_mode=1 and actual_flag=1"
                cur.execute(sSql)
                rsPayType = cur.fetchall()
                if len(rsPayType) == 0:
                    raise Exception("业务账款调整失败：获取现金支付方式失败.")
                rtn = self._generatePay(conn, rdBill["busi_type"], [{"pay_type": rsPayType[0][0], "pay_amt": real_amt}], ac_bill=iBill, acDate=acDate)
                if not rtn["result"]:
                    raise Exception("业务账款调整失败：" + rtn["info"])

            conn.commit()
            rtnData["result"] = True
            rtnData["info"] = adj_remark + "操作成功."
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def fBusiRemove(self, ac_bill, remark, acDate=datetime.now()):
        """
        解除账单
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
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "busi_bill", "third_bill", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "close_flag"]
            sCols = ""
            for item in lCol:
                sCols += ", " + item
            sCols = sCols[2:len(sCols)]
            sSql = r"select {sCols} from ac_bill_flow where id={ac_bill}".format(sCols=sCols, ac_bill=ac_bill)
            cur.execute(sSql)
            rsBill = cur.fetchall()
            if len(rsBill) == 0:
                raise Exception("账单ID[{ac_bill}]无效.".format(ac_bill=ac_bill))
            rdBill = dict(zip(lCol, rsBill[0]))
            if rdBill["out_ac"] != self.o_id:
                raise Exception("账单[{ac_bill}]不属于{en_type}{en_name}.".format(
                    ac_bill=ac_bill, 
                    en_type=AppFinance.c_data["实体类型"][self.o_entity_type]),
                    en_name=self.o_simple_name
                )
            if rdBill["frush_flag"] == 1:
                raise Exception("账单[{ac_bill}]是冲红单，不可冲红.".format(ac_bill=ac_bill))
            elif rdBill["frush_flag"] == 2:
                raise Exception("账单[{ac_bill}]已被冲红，不可冲红.".format(ac_bill=ac_bill))
            
            # 商家支付平台佣金冲红
            sSql = r"select id from ac_pay_flow where bill_flow={bill_id}".format(bill_id=ac_bill)
            cur.execute(sSql)
            rsPay = cur.fetchall()
            for line in rsPay:
                rtn = self.fFrushPay(line[0], frush_remark)
                if not rtn["result"]:
                    raise Exception(rtn["info"])
            
            # 商家应付平台佣金冲红
            rtn = self.fFrushBill(ac_bill, remark, acDate=acDate)
            if not rtn["result"]:
                raise Exception(rtn["info"])

            conn.commit()
            rtnData["result"] = True
        except Exception as e:
            if bConn:
                conn.rollback()
            rtnData["info"] = str(e)
        finally:
            if bConn:
                conn.close()

        return rtnData

    def _generateBill(self, conn, busi_type, orig_amt, real_amt, busi_summary, acObj=None, busi_bill=None, third_bill=None, acDate=datetime.now()):
        """
        生成账单
        conn                数据库连接
        busi_type           业务类型 4:安装 5:售后
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
            if busi_type in (1,2,3):
                raise Exception("账户类操作不需要生成账单.")
            if real_amt <= 0 or not real_amt:
                raise Exception("交易金额无效.")
            if real_amt > 999999.99:
                raise Exception("充值金额太大：{amt}，请检查是否正确".format(amt=real_amt))
            if not busi_summary or len(busi_summary) == 0:
                raise Exception("业务摘要无效.")
            if not acObj:
                raise Exception("请指定交易对方账号.")

            # 数据初始化
            if not self.bInit:
                rtn = self.dataRefresh()
                if not rtn["result"]:
                    raise Exception("查询应付账户数据失败：" + rtn["info"])
            if not acObj.bInit:
                rtn = acObj.dataRefresh()
                if not rtn["result"]:
                    raise Exception("获取应收账户信息失败：" + rtn["info"])

            # 获取游标
            cur = conn.cursor()

            # 记录账单明细
            sSql = r"insert into ac_bill_flow ( out_ac, out_simple, in_ac, in_simple, ac_type, busi_type, busi_bill, third_bill, orig_amt, real_amt, ac_date, busi_summary ) " + \
                r"values ({out_ac}, '{out_simple}', {in_ac}, '{in_simple}', {ac_type}, {busi_type}, '{busi_bill}', '{third_bill}', {orig_amt}, {real_amt}, '{ac_date}', '{busi_summary}' ) ".format(
                    out_ac=self.o_id,
                    out_simple=self.o_simple_name,
                    in_ac=acObj.o_id,
                    in_simple=acObj.o_simple_name,
                    ac_type=self.o_ac_type,
                    busi_type=busi_type,
                    busi_bill=busi_bill if busi_bill else '',
                    third_bill=third_bill if third_bill else '',
                    orig_amt=orig_amt,
                    real_amt=real_amt,
                    ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                    busi_summary=busi_summary
                )
            cur.execute(sSql)
            iFlow = conn.insert_id()

            if busi_type > 3 and self.o_ac_type == 3:
                # 信用额度减少
                sSql = r"update ac_account set credit_amt = credit_amt - {amt} where id={id}".format(
                    id=self.o_id,
                    amt=real_amt
                )
                num = cur.execute(sSql)
                if num != 1:
                    raise Exception("账户{name}数据异常.".format(name=self.o_name))

            # 返回数据
            rtnData["result"] = True
            rtnData["dataNumber"] = iFlow
            rtnData["info"] = busi_summary + "成功."
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _generatePayAc(self, conn, busi_type, paylist, pay_time=datetime.now(), acDate=datetime.now()):
        """
        账户支付
        conn                数据库连接
        busi_type           业务类型 1:充值 2:提现 3:还款
        paylist             付款列表
            pay_type        付款方式
            pay_amt         付款金额
        pay_time            支付时间 为空时取当前服务器时间
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
            for line in paylist:
                if line["pay_type"] <= 0:
                    raise Exception("支付类型[{pay_type}]无效.".format(pay_type=line["pay_type"]))
                if not line["pay_amt"] or line["pay_amt"] <= 0:
                    raise Exception("支付金额无效.")

            # 账户操作，刷新余额
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception("查询账户数据失败：" + rtn["info"])
            
            # 获取游标
            cur = conn.cursor()

            # 计算支付总额
            paySum = 0
            for line in paylist:
                paySum += line["pay_amt"]

            # 检查账户余额
            if busi_type == 2 and paySum > self.o_ac_balance:
                raise Exception("余额不足.")

            iFlow = []
            for line in paylist:
                # 插入支付记录
                if busi_type == 1 or busi_type == 3:
                    sSql = r"insert into ac_pay_flow ( in_ac, in_simple, in_balance, busi_type, bill_flow, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                        r"values ({in_ac}, '{in_simple}', {in_balance}, {busi_type}, {bill_flow}, {pay_type}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                            in_ac=self.o_id,
                            in_simple=self.o_simple_name,
                            in_balance=self.o_ac_balance + line["pay_amt"],
                            busi_type=busi_type,
                            bill_flow="NULL",
                            pay_type=line["pay_type"],
                            pay_amt=line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary='充值' if busi_type == 1 else '还款'
                        )
                elif busi_type == 2:
                    sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, busi_type, bill_flow, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                        r"values ({out_ac}, '{out_simple}', {out_balance}, {busi_type}, {bill_flow}, {pay_type}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                            out_ac=self.o_id,
                            out_simple=self.o_simple_name,
                            out_balance=self.o_ac_balance - line["pay_amt"],
                            busi_type=busi_type,
                            bill_flow="NULL",
                            pay_type=line["pay_type"],
                            pay_amt=line["pay_amt"],
                            pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                            ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                            busi_summary='提现'
                        )
                cur.execute(sSql)
                iFlow.append(conn.insert_id())

            if busi_type == 1:
                # 余额增加
                sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id}".format(
                    id=self.o_id,
                    amt=paySum
                )
            elif busi_type == 2:
                # 余额减少
                sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id}".format(
                    id=self.o_id,
                    amt=paySum
                )
            elif busi_type == 3:
                # 余额增加、信用额度增加
                sSql = r"update ac_account set ac_balance = ac_balance + {amt}, credit_amt = credit_amt + {amt} where id={id}".format(
                    id=self.o_id,
                    amt=paySum
                )
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账户{name}数据异常.".format(name=self.o_name))
            
            # 信用账户自动支付账单
            if busi_type == 3:
                rtn = self._creditAutoPay(conn)
                if not rtn["result"]:
                    raise Exception(rtn["info"])

            # 返回数据
            rtnData["result"] = True
            rtnData["entities"] = iFlow
            rtnData["info"] = "账单{id}支付完成.".format(id=ac_bill)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _generatePayBill(self, conn, busi_type, paylist, ac_bill=None, pay_time=datetime.now(), acDate=datetime.now()):
        """
        账单支付
        conn                数据库连接
        busi_type           业务类型 4:安装 5:售后
        paylist             付款列表
            pay_type        付款方式
            pay_amt         付款金额
        ac_bill             账单ID
        pay_time            支付时间 为空时取当前服务器时间
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
            if not AppFinance.c_data["业务类型"].get(busi_type) or busi_type < 4:
                raise Exception("账单支付不支持的业务类型[{busi_type}].".format(busi_type=busi_type)) 
            if not ac_bill:
                raise Exception("请指定账单ID.")
            if len(paylist) == 0:
                raise Exception("没有指定支付类型.")
            for line in paylist:
                if line["pay_type"] <= 0:
                    raise Exception("支付类型[{pay_type}]无效.".format(pay_type=line["pay_type"]))
                if not line["pay_amt"] or line["pay_amt"] <= 0:
                    raise Exception("支付金额无效.")

            # 支付业务需刷新余额
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception("查询账户数据失败：" + rtn["info"])
            
            # 获取游标
            cur = conn.cursor()

            # 计算支付总额
            paySum = 0
            for line in paylist:
                paySum += line["pay_amt"]
            
            # 获取原账单信息
            lCol = ["out_ac", "out_simple", "in_ac", "in_simple", "ac_type", "busi_type", "orig_amt", "real_amt", "ac_date", "busi_summary", "frush_flag", "pay_flag", "check_flag", "close_flag"]
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
                lData = dict(zip(lCol, rs[0]))
                if lData["out_ac"] != self.o_id:
                    raise Exception("账单[{bill}]不属于{entity_type}账户{name}.".format(
                        bill=ac_bill, 
                        entity_type=AppFinance.c_data["实体类型"][self.o_entity_type],
                        name=self.o_name
                    ))
            else:
                raise Exception("账单ID[{id}]出现重复异常.".format(id=ac_bill))
            if busi_type != lData["busi_type"]:
                raise Exception("业务类型[{busi_type}]与账单[{bill_type}]不匹配.".format(busi_type=busi_type, bill_type=lData["busi_type"]))
            if lData["pay_flag"] == 1:
                raise Exception("账单ID[{id}]已支付，不可重复支付.".format(id=ac_bill))
            elif lData["pay_flag"] == 2:
                raise Exception("账单ID[{id}]无需支付.".format(id=ac_bill))
            
            # 检查业务类型是否匹配
            if lData["busi_type"] != busi_type:
                raise Exception("支付业务类型[{busi_type1}]与账单业务类型[{busi_type2}]不匹配.".format(
                    busi_type1=AppFinance.c_data["业务类型"][busi_type],
                    busi_type2=AppFinance.c_data["业务类型"][lData["busi_type"]]
                ))
            
            # 检查支付金额是否匹配
            if paySum != lData["real_amt"]:
                raise Exception("支付金额必须等于应付金额.")
            
            # 收款账号信息
            if lData["in_ac"]:
                acObj = AppFinance(AppFinance.c_sett, 1, {"type": 1, "acID": lData["in_ac"]})
                if acObj.o_ac_type > 1:
                    raise Exception("{ac_type}账户不可用于收款.".format(ac_type=AppFinance.c_data["记账类型"][acObj.o_ac_type]))
            else:
                raise Exception("请指定收款账户.")

            # 检查账户余额
            if self.o_ac_type > 1 and paySum > self.o_ac_balance:
                raise Exception("余额不足.")

            iFlow = []
            for line in paylist:
                # 插入支付记录
                sSql = r"insert into ac_pay_flow ( out_ac, out_simple, out_balance, in_ac, in_simple, in_balance, bill_flow, busi_type, pay_type, pay_amt, pay_time, ac_date, busi_summary, frush_flag, check_flag ) " + \
                    r"values ({out_ac}, '{out_simple}', {out_balance}, {in_ac}, '{in_simple}', {in_balance}, {bill_flow}, {busi_type}, {pay_type}, {pay_amt}, '{pay_time}', '{ac_date}', '{busi_summary}', 0, 0)".format(
                        out_ac=self.o_id,
                        out_simple=self.o_simple_name,
                        out_balance=self.o_ac_balance - line["pay_amt"],
                        in_ac=acObj.o_id,
                        in_simple=acObj.o_simple_name,
                        in_balance=acObj.o_ac_balance + line["pay_amt"],
                        bill_flow=ac_bill,
                        busi_type=busi_type,
                        pay_type=line["pay_type"],
                        pay_amt=line["pay_amt"],
                        pay_time=datetime.strftime(acDate, '%Y-%m-%d %H:%M:%S'),
                        ac_date=datetime.strftime(acDate, '%Y-%m-%d'),
                        busi_summary="支付：" + lData["busi_summary"]
                    )
                cur.execute(sSql)
                iFlow.append(conn.insert_id())

            # 余额更新
            sSql = r"update ac_account set ac_balance = ac_balance - {amt} where id={id}".format(
                id=self.o_id,
                amt=paySum
            )
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账户{name}数据异常.".format(name=self.o_name))
            sSql = r"update ac_account set ac_balance = ac_balance + {amt} where id={id}".format(
                id=acObj.o_id,
                amt=paySum
            )
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账户{name}数据异常.".format(name=self.o_name))

            # 更新账单支付标志
            sSql = r"update ac_bill_flow set pay_flag=1 where id={id}".format(id=ac_bill)
            num = cur.execute(sSql)
            if num != 1:
                raise Exception("账户{name}数据异常.".format(name=self.o_name))

            # 返回数据
            rtnData["result"] = True
            rtnData["entities"] = iFlow
            rtnData["info"] = "账单{id}支付完成.".format(id=ac_bill)
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            pass

        return rtnData

    def _creditAutoPay(self, conn):
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

        try:
            # 数据刷新
            rtn = self.dataRefresh()
            if not rtn["result"]:
                raise Exception(rtn["info"])

            # 余额检查
            if not self.o_ac_balance > 0:
                raise Exception("余额不足.")
            
            # 获取游标
            cur = conn.cursor()

            # 信用账户自动完成支付
            acObj = AppFinance(AppFinance.c_sett, 1, {"type": 2, "enType": self.o_other_en_type, "enID": self.o_other_en_id, "acType": 1})
            # 获取支付方式
            sSql = r"select id from ac_pay_type where pay_mode=3 and actual_flag=1"
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
                if len(rs) > 0:
                    lData = [dict(zip(lCol, line)) for line in rs]
                    for line in lData:
                        if line["real_amt"] > self.o_ac_balance:
                            break
                        rtn = self._generatePay(conn, line["busi_type"], [{"pay_type":rsPayType[0][0], "pay_amt":line["real_amt"]}], line["id"])
                        if not rtn["result"]:
                            raise Exception(rtn["info"])
                        iNum += 1
                        dPaid += line["real_amt"]
                else:
                    bContinue = False
            rtnData["result"] = True
            rtnData["info"] = "共完成{num}笔合计{amt}账单的支付.".format(num=iNum, amt=dPaid)
        except Exception as e:
            rtnData["info"] = str(e)
        
        return rtnData


if __name__ == "__main__":
    import os
    from appConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")
