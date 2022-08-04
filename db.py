# encoding = utf-8

"""
   @describe: 实现mysql数据库操作
"""
import MySQLdb.constants.FIELD_TYPE

from conf.mysql_conf import *

import pymysql

class Mysql:
    _con_db = None   # 数据库
    _cursor = None  # 游标
    Field_TYPE = {}

    def __init__(self):
        for j in [i for i in dir(MySQLdb.constants.FIELD_TYPE) if "__" not in i and "ENUM" != i and "CHAR" != i]:
            self.Field_TYPE[eval("MySQLdb.constants.FIELD_TYPE." + j)] = j

    def connect(self, host=None, db=None, user=None, password=None, port=None):
        """
        pymysql连接数据库
        :param port:
        :param host:
        :param db:
        :param user:
        :param password:
        :return:
        """
        if host is None:
            host = DB_HOST
        if db is None:
            db = DB_DATABASE
        if user is None:
            user = DB_USER
        if password is None:
            password = DB_PASSWORD
        if port is None:
            port = DB_PORT
        self._con_db = pymysql.connect(
            host=host,
            database=db,
            user=user,
            port=port,
            password=password,
            charset="utf8"
        )
        self._cursor = self._con_db.cursor()

    def select(self, sql):
        """
        数据库查询操作
        :param sql:
        :param cols_name:
        :return: df
        """
        try:
            c = self._cursor.execute(sql)
            info = self._cursor.description
            if c == 0:
                return {
                "data": None,
                "info": info
            }
            data = list(self._cursor.fetchall())
            return {
                "data": data,
                "info": info
            }
        except:
            print("Error: unable to fetch data")
            return None

    def getTableInfo(self, table):
        """
        获取表信息
        :param table:
        :return:
        """
        info = self.select("select * from {} limit 0".format(table))["info"]
        return info

    def changeDatabase(self, database_name):
        """
        改变当前所在数据库
        :param database_name:
        :return:
        """
        try:
            self._cursor.execute("USE {}".format(database_name))
            return True
        except Exception as e:
            print("更改数据库失败！, {}".format(database_name, e))
            return False

    def close(self):
        """
        关闭连接
        :return:
        """
        self._cursor.close()
        self._con_db.close()


if __name__ == "__main__":
    d = Mysql()
    d.connect()
    a = d.select("select * from consumption limit 3")
    print(a)
    # print(d.getTableInfo("consumption"))
    d.close()

