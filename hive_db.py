# enncoding = utf-8

"""
    @describe: 实现hive操作
"""

from pyhive import hive
from conf.hive_conf import *
from db import Mysql

class HiveDb:
    _con_db = None  # 数据库
    _cursor = None  # 游标
    # mysql类型对应数据类型
    hive_type = {}

    def __init__(self):
        m = Mysql()
        self.hive_type = dict(zip(m.Field_TYPE.values(), m.Field_TYPE.values()))
        self.hive_type["LONG"] =  "BIGINT"
        self.hive_type["STRING"] = "VARCHAR(256)"
        self.hive_type["DATETIME"] = "TIMESTAMP"
        del m
        print(self.hive_type.items())

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
            host = HIVE_HOST
        if db is None:
            db = HIVE_DATABASE
        if user is None:
            user = HIVE_USER
        if password is None:
            password = HIVE_PASSWORD
        if port is None:
            port = HIVE_PORT
        self._con_db = hive.Connection(host=host,
                                       port=port,
                                       username=user,
                                       password=password,
                                       database=db)
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

    def getTableInfo(self, table):
        """
        获取表信息
        :param table:
        :return:
        """
        info = self.select("select * from {} limit 0".format(table))["info"]
        return info

    def createDatabase(self, database_name):
        """
        创建数据库
        :param database_name: 数据库名
        :return:
        """
        try:
            self._cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
            return True
        except Exception as e:
            print("创建{}失败, {}".format(database_name, e))
            return False

    def createTable(self, table_name, fields):
        """
        创建表
        :param table_name: 表名
        :param fields: type:list, [(col_name type)]
        :return:
        """
        try:
            # 构建语句
            sql = "CREATE TABLE IF NOT EXISTS {} ( ".format(table_name)
            for i in range(len(fields)):
                for j in range(2):
                    if i == len(fields) - 1:
                        sql += "{} {}) ".format(fields[i][0], fields[i][1])
                    else:
                        sql += "{} {}, ".format(fields[i][0], fields[i][1])
                    break
            sql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' STORED AS TEXTFILE"
            # print(sql)
            self._cursor.execute(sql)
            return True
        except Exception as e:
            print("创建表{}失败, {}".format(table_name, e))
            return False

    def exec(self, sql):
        """
        执行sql,除数据更新数据
        :param sql:
        :return:
        """
        try:
            self._cursor.execute(sql)
            return True
        except Exception as e:
            print("执行失败sql:{}\n, {}".format(sql, e))
            return False

    def deleteTable(self, table_name):
        """
            删除表
            :param table_name: 表名
            :return:
        """
        try:
            self._cursor.execute("  DROP TABLE IF EXISTS {}".format(table_name))
            return True
        except Exception as e:
            print("删除表{}失败, {}".format(table_name, e))
            return False

    def insertDATA(self, table_name, data, partition=None, flag=False):
        """
        插入数据
        :param partition: 分区条件 String
        :param data: Sring
        :param table_name:
        :param flag: 为真时插入分区数据
        :return:
        """
        try:
            sql = None
            sql1 = "INSERT INTO TABLE {} PARTITION ({}) VALUES ({})"
            sql2 = "INSERT INTO TABLE {} VALUES ({})"
            if flag:
                sql = sql1.format(table_name, partition, data)
            else:
                sql = sql2.format(table_name, data)
            print(sql)
            self._cursor.execute(sql)
            self._con_db.commit()
        except Exception as e:
            # self._con_db.rollback()
            print("插入失败， {}".format(e))
            self._con_db.rollback()

    def deleteDatabase(self, database_name):
        """
            删除数据库
            :param database_name: 数据库名
            :return:
        """
        try:
            self._cursor.execute(" DROP DATABASE IF EXISTS {} CASCADE".format(database_name))
            return True
        except Exception as e:
            print("删除库{}失败, {}".format(database_name, e))
            return False

    def close(self):
        """
        关闭连接
        :return:
        """
        self._cursor.close()
        self._con_db.close()

if __name__ == "__main__":
    hd = HiveDb()
    hd.connect()
    d = hd.select('show tables')
    print(d)
    hd.createTable("aaa", [("a", "int"), ("b", "String")])
    hd.close()