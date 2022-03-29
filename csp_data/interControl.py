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
        if self.sett.dataAccess == "db":
            rtnData = self._get_jos_homefw_task_db()
        else:
            rtnData = self._get_jos_homefw_task_request()
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

    def _put_jdwork_to_3c(self, sFile):
        """
        把数据Excel文件批量导入3C接口
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }
        import requests
        import string
        from requests_toolbelt import MultipartEncoder

        sBase = "https://szkzkj.3cerp.com"
        sUrl = "/pages/net/importExcelToNetOrders.htm"

        # sCookie = r"_ati=5370730286794; _jdeid=N5U4FUMVIXC74VXTTFD2BFBYDL5N25RNM7F7EQRAKW2B5HKQKLX4NB3LCWWKEHEUR56AMLN7EGUKDNIWARQLH6HDXM; JSESSIONID=31D1EA7EEEB2B1E9BEE26557C70B7799; 3cu=9daa4732c46ec4e0480668b8e0269128; acw_tc=3ccdc16516463751257708174e76a76d43e9e05dca8865a5d246c706396319; 3AB9D23F7A4B3C9B=N5U4FUMVIXC74VXTTFD2BFBYDL5N25RNM7F7EQRAKW2B5HKQKLX4NB3LCWWKEHEUR56AMLN7EGUKDNIWARQLH6HDXM"

        try:
            if self.sett.dataAccess == "db":
                rtn = self._get_cookie_from3c_db()
            else:
                rtn = self._get_cookie_from3c_request()
            if rtn["result"]:
                sCookie = rtn["dataString"]
            else:
                raise Exception("没找到有效Cookie.")

            # 向3C发送数据
            dParm = {}
            dParm["import_platform"] = "other"
            dParm["import_shop_id"] = "55"
            lFile = os.path.split(sFile)
            filename = lFile[len(lFile) - 1]
            f=open(sFile,'rb')
            files=[('orderExcelFile',(filename,f,'application/vnd.ms-excel'))]

            res = requests.request("POST", sBase + sUrl, headers={'Cookie': sCookie}, data=dParm, files=files)
            rtn = self._is_json(res.text)
            if not rtn:
                raise Exception("请指定请求Cookie.")

            if not rtn["result"]:
                rtnData["info"] = rtn["msg"]

            rtnData["result"] = True
        except Exception as e:
            rtnData["info"] = str(e)
            self.sett.logger.error(rtnData["info"])
        finally:
            f.close()

        return rtnData

    def _get_cookie_from3c_request(self):
        """
        获取3C网站cookie
        方式：http请求
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }
        import requests

        try:
            url = "https://214653365acf41ae9fcfc4783c1f6045-cn-hangzhou.alicloudapi.com/getcookie?AppCode=14de611f25394da694479c77570aeace"

            payload={}
            headers = {}

            res = requests.request("GET", url, headers=headers, data=payload)
            rtn = self._is_json(res.text)
            if not rtn:
                raise Exception("aliyun API服务请求失败.")

            rsRec = rtn["result"]
            if len(rsRec) == 0:
                raise Exception("没有找到可用的cookie.")
            rtnData["result"] = True
            rtnData["dataString"] = rsRec[random.randint(0, len(rsRec) - 1)]["cookie"]
        except Exception as e:
            rtnData["info"] = str(e)
        
        return rtnData

    def _get_cookie_from3c_db(self):
        """
        获取3C网站cookie
        方式：数据库直连
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        ibConnHandle = False
        try:
            # 获取连接信息
            connHandle = self.dbJDSrv.GetConnect()
            ibConnHandle = True
            curHandle = connHandle.cursor()
            
            ls_sql = r"SELECT cookie FROM spider.c3_account_cookie where status = 1 and delete_flag = 0"
            curHandle.execute(ls_sql)
            rsTmp = curHandle.fetchall()
            if len(rsTmp) > 0:
                sCookie = rsTmp[random.randint(0, len(rsTmp) - 1)][0]
            else:
                raise Exception("没找到有效Cookie.")
            rtnData["result"] = True
            rtnData["dataString"] = sCookie
        except Exception as e:
            rtnData["info"] = str(e)
        finally:
            if ibConnHandle:
                connHandle.close()

        return rtnData

    def _get_jos_homefw_task_request(self):
        """
        从Wukong京东居家库获取京东工单
        方式：http请求
        店铺=京东自营
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "dataDatetime": None, # 日期时间
            "info": "",  # 信息
            "entities": {}
        }

        # 测试模式：当测试模式时，会写入假数据
        ibTest = False

        try:
            # 获取京东数据
            # --查询语句
            if not ibTest:
                iBreakPoint = self.sett.jdBillBreakpoint
            else:
                iBreakPoint = self.sett.jdBillBreakpoint - 1

            import requests

            url = r"https://214653365acf41ae9fcfc4783c1f6045-cn-hangzhou.alicloudapi.com/get/neworder/jd?AppCode=14de611f25394da694479c77570aeace&ibreakpoint="
            url += str(iBreakPoint)

            payload={}
            headers = {}

            res = requests.request("GET", url, headers=headers, data=payload)
            rtn = self._is_json(res.text)
            if not rtn:
                raise Exception("aliyun API服务请求失败.")

            rsBill = rtn["result"]
            if len(rsBill) > 0:
                rsBill = [item for item in rsBill if item["ware_jd"] in InterControl.c_skuMap]
                dtNew = datetime.strptime("2022-01-01 01:01:01", "%Y-%m-%d %H:%M:%S")
                for line in rsBill:
                    # 把JSON字符串转换成正确的数据类型
                    line["price"] = float(line["price"])
                    line["qty"] = float(line["qty"])
                    line["freight"] = float(line["freight"])
                    line["created_time"] = datetime.strptime(line["created_time"], "%Y-%m-%d %H:%M:%S")
                    line["paid_time"] = datetime.strptime(line["paid_time"], "%Y-%m-%d %H:%M:%S")
                    line["create_timestamp"] = datetime.strptime(line["create_timestamp"], "%Y-%m-%d %H:%M:%S.%f")
                    line["dis_shop"] = float(line["dis_shop"])
                    line["dis_platform"] = float(line["dis_platform"])
                    # 替换成3C商品
                    if line["ware_jd"] in InterControl.c_skuMap:
                        line["ware_code"] = InterControl.c_skuMap[line["ware_jd"]][0]
                        line["ware_name"] = InterControl.c_skuMap[line["ware_jd"]][1]
                        # 获取最大时间戳
                        if line["create_timestamp"] > dtNew:
                            dtNew = line["create_timestamp"]
                        line.pop("ware_jd")
                        line.pop("create_timestamp")
                        if ibTest:
                            line["order_no"] = "88888888"
                            line["buyer_nick"] = "测试单"
                            line["buyer_name"] = "测试单"
                            line["buyer_mobile"] = "13888888888"
                            line["seller_remark"] = "注意：这是测试单"
                    else:
                        raise Exception("京东商品{item}不能匹配到进销存商品.".format(item=str(line["ware_id"])))
                
                # --插入配件商品
                icnt = len(rsBill)
                for iTmp in range(icnt):
                    for line in InterControl.c_skuSub[rsBill[iTmp]["ware_code"]]:
                        iSub = rsBill[iTmp].copy()
                        iSub["ware_code"] = line[0]
                        iSub["ware_name"] = line[1]
                        rsBill.append(iSub)
                rsBill = sorted(rsBill, key=lambda keys:keys["order_no"])

                # --写入Excel
                ldCol = ['order_no', 'buyer_nick', 'buyer_name', 'buyer_mobile', 'buyer_province', 'buyer_city', 'buyer_district', 'buyer_address']
                ldCol.extend(['buyer_zip', 'ware_name', 'ware_code', 'sale_attr', 'price', 'qty', 'express_no', 'freight', 'created_time'])
                ldCol.extend(['paid_time', 'buyer_remark', 'seller_remark', 'dis_shop', 'dis_platform'])
                ldTitle = ["订单号*", "购买人昵称*", "收件人姓名*", "手机*", "省*", "市*", "区*", "地址*"]
                ldTitle.extend(["邮编", "物品名称*", "商品编码*", "销售属性", "单价*", "数量*", "快递单号", "运费", "下单时间*"])
                ldTitle.extend(["付款时间*", "买家备注", "卖家备注", "店铺折扣", "平台折扣"])
                rtnData = self._dsToExcel(rsBill, ldTitle, ldCol)
                if rtnData["result"]:
                    rtnData["entities"] = rsBill
                    rtnData["dataNumber"] = icnt
                    rtnData["dataDatetime"] = dtNew
            else:
                rtnData["result"] = True
                rtnData["dataNumber"] = 0
        except Exception as e:
            rtnData["info"] = str(e)
            self.sett.logger.error(rtnData["info"])
        finally:
            pass

        return rtnData

    def _get_jos_homefw_task_db(self):
        """
        从Wukong京东居家库获取京东工单
        方式：直连数据库
        店铺=京东自营
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "dataDatetime": None, # 日期时间
            "info": "",  # 信息
            "entities": {}
        }

        # 测试模式：当测试模式时，会写入假数据
        ibTest = False

        ibConnJDSrv = False
        try:
            connJDSrv = self.dbJDSrv.GetConnect()
            ibConnJDSrv = True
            curJDSrv = connJDSrv.cursor()

            # 获取京东数据
            # --查询语句
            if not ibTest:
                iBreakPoint = self.sett.jdBillBreakpoint
            else:
                iBreakPoint = self.sett.jdBillBreakpoint - 1
            lsSql = "select sale_order_no, user_name, user_name, user_mobile, user_province, user_city, user_county, user_address, " + \
                "'', sku, '', '', '', 0, num, '', 0, create_date, appoint_date, create_time, '', '', null, null " + \
                "from jos_homefw_task " + \
                "where store_type=2 " + \
                "and order_status=1 " + \
                "and truncate(cast(create_time as decimal(20,3)), 3) > " + str(iBreakPoint) + " " + \
                "order by create_time asc "
            # "or sale_order_no in ('233931974708', '233549316944) " + \
            # 测试单据：('233931974708', '233549316944')
            # --数据列名
            ldCol = ['order_no', 'buyer_nick', 'buyer_name', 'buyer_mobile', 'buyer_province', 'buyer_city', 'buyer_district', 'buyer_address']
            ldCol.extend(['buyer_zip', 'ware_jd', 'ware_name', 'ware_code', 'sale_attr', 'price', 'qty', 'express_no', 'freight', 'created_time'])
            ldCol.extend(['paid_time', 'create_timestamp', 'buyer_remark', 'seller_remark', 'dis_shop', 'dis_platform'])
            curJDSrv.execute(lsSql)
            rsTmp = curJDSrv.fetchall()
            if len(rsTmp) > 0:
                rsBill = [dict(zip(ldCol, line)) for line in rsTmp]
                rsBill = [item for item in rsBill if item["ware_jd"] in InterControl.c_skuMap]
                dtNew = datetime.strptime("2022-01-01 01:01:01", "%Y-%m-%d %H:%M:%S")
                for line in rsBill:
                    # 替换成3C商品
                    if line["ware_jd"] in InterControl.c_skuMap:
                        line["ware_code"] = InterControl.c_skuMap[line["ware_jd"]][0]
                        line["ware_name"] = InterControl.c_skuMap[line["ware_jd"]][1]
                        # 获取最大时间戳
                        if line["create_timestamp"] > dtNew:
                            dtNew = line["create_timestamp"]
                        line.pop("ware_jd")
                        line.pop("create_timestamp")
                        if ibTest:
                            line["order_no"] = "88888888"
                            line["buyer_nick"] = "测试单"
                            line["buyer_name"] = "测试单"
                            line["buyer_mobile"] = "13888888888"
                            line["seller_remark"] = "注意：这是测试单"
                    else:
                        raise Exception("京东商品{item}不能匹配到进销存商品.".format(item=str(line["ware_id"])))
                
                # --插入配件商品
                icnt = len(rsBill)
                for iTmp in range(icnt):
                    for line in InterControl.c_skuSub[rsBill[iTmp]["ware_code"]]:
                        iSub = rsBill[iTmp].copy()
                        iSub["ware_code"] = line[0]
                        iSub["ware_name"] = line[1]
                        rsBill.append(iSub)
                rsBill = sorted(rsBill, key=lambda keys:keys["order_no"])

                # --写入Excel
                ldCol.remove("ware_jd")
                ldCol.remove("create_timestamp")
                ldTitle = ["订单号*", "购买人昵称*", "收件人姓名*", "手机*", "省*", "市*", "区*", "地址*"]
                ldTitle.extend(["邮编", "物品名称*", "商品编码*", "销售属性", "单价*", "数量*", "快递单号", "运费", "下单时间*"])
                ldTitle.extend(["付款时间*", "买家备注", "卖家备注", "店铺折扣", "平台折扣"])
                rtnData = self._dsToExcel(rsBill, ldTitle, ldCol)
                if rtnData["result"]:
                    rtnData["entities"] = rsBill
                    rtnData["dataNumber"] = icnt
                    rtnData["dataDatetime"] = dtNew
            else:
                rtnData["result"] = True
                rtnData["dataNumber"] = 0
        except Exception as e:
            rtnData["info"] = str(e)
            self.sett.logger.error(rtnData["info"])
            if ibConnJDSrv:
                connJDSrv.rollback()
        finally:
            if ibConnJDSrv:
                connJDSrv.close()

        return rtnData

    def _update_jd_breakpoint(self, newPoint):
        """
        更新京东工单传输断点
        """
        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        try:
            # 更新数据截点
            sPoint = datetime.strftime(newPoint, '%Y%m%d%H%M%S') + "." + datetime.strftime(newPoint, '%f')[0:3]
            self.sett.config.set("business", "jdBillBreakpoint", sPoint)
            self.sett.config.write(open(os.path.join(self.sett.path, self.sett.file), "w"))
            self.sett.jdBillBreakpoint = float(sPoint)
            rtnData["result"] = True
            rtnData["info"] = "成功更新京东工单传输截点."
        except Exception as e:
            rtnData["info"] = str(e)
            self.sett.logger.error(rtnData["info"])

        return rtnData

    def _dsToExcel(self, ds, lsTitle, lsCode):
        """
        数据导出到Excel
        """
        import xlwt

        rtnData = {
            "result": False,  # 逻辑控制 True/False
            "dataString": "",  # 字符串
            "dataNumber": 0,  # 数字
            "info": "",  # 信息
            "entities": {}
        }

        fWb = xlwt.Workbook()
        sheet = fWb.add_sheet("sheet 1")
        col = 0
        for item in lsTitle:
            sheet.write(0, col, item)
            col += 1
        row = 1
        for line in ds:
            col = 0
            for item in lsCode:
                if isinstance(line[item], datetime):
                    sheet.write(row, col, datetime.strftime(line[item], "%Y-%m-%d %H:%M:%S"))
                else:
                    sheet.write(row, col, line[item])
                col += 1
            row += 1
        sFile = os.path.join(self.sett.tmpFilePath, "tmp_jdbill" + datetime.now().strftime("%Y%m%d%H%M%S") + ".xls")
        try:
            fWb.save(sFile)
            rtnData["result"] = True
            rtnData["dataString"] = sFile
            rtnData["info"] = "导出Excel成功."
        except Exception as e:
            rtnData["info"] = str(e)
            self.sett.logger.error(rtnData["info"])

        return rtnData

    def _is_json(self, myjson):
        """
        判断字符串是否是JSON格式
        """
        import json
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            self.sett.logger.error(str(e))
            return False
        return json_object

    def _getDay(self, iTime):
        """
        获取时间戳日
        :param iTime:
        :return:
        """
        return int(time.strftime("%Y%m%d", time.localtime(iTime)))

    def _getHour(self, iTime):
        """
        获取时间戳小时
        :return:
        """
        return int(time.strftime("%H", time.localtime(iTime)))


if __name__ == "__main__":
    import os
    from interConfig import Settings

    myPath = os.path.abspath(os.path.dirname(__file__))
    mySett = Settings(myPath, "config")

    myInter = InterControl(mySett)
    rtn = myInter.trans_jos_homefw_task()
    print(rtn["info"]) 
