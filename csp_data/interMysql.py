# -*- coding:utf-8 -*-
"""
"""
__author__ = "Cliff.wang"
import pymysql
pymysql.version_info = (1, 4, 13, "final", 0)
pymysql.install_as_MySQLdb()
import decimal      #打包需要

class MYSQL:
    def __init__(self, host, port, user, pwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = pwd
        self.db = db

    def GetConnect(self):
        if not self.db:
            raise Exception("没有配置数据库信息")
        conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.db, charset="utf8")
        if not conn:
            raise Exception("数据库[{server}][{db}]连接失败".format(server=self.host, db=self.db))
        else:
            return conn

    def __GetCursor(self):
        if not self.db:
            raise Exception("没有配置数据库信息")
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise Exception("数据库[{server}][{db}]连接失败".format(server=self.host, db=self.db))
        else:
            return cur

    def ExecQuery(self, sql):
        cur = self.__GetCursor()
        cur.execute(sql)
        resList = cur.fetchall()

        self.conn.close()
        return resList

    def ExecNonQuery(self, sql):
        cur = self.__GetCursor()
        cur.execute(sql)

        self.conn.commit()
        self.conn.close()

if __name__ == "__main__":
    if 1 == 1:
        my = MYSQL(host="192.168.10.159", port=3308, user="wanglinqun", pwd="sz@mgf#$%11125.115900", db="unlocker")
        lsSql = r"select * from ware where id = 1"
        ds = my.ExecQuery(lsSql)
        i = 1