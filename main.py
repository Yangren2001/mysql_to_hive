# encoding = utf-8

"""
   @describe: 主函数
"""

from db import Mysql
from hive_db import HiveDb

def main():
    m = Mysql()
    hd = HiveDb()
    m.connect()
    hd.connect()
    # m.changeDatabase()  # 改变当前选择数据库
    t = m.select("show tables")
    database_now = m.select("select database()")["data"][0][0]  # 当前数据库
    hd.deleteTable(database_now)
    hd.createDatabase(database_now)  # 建立hive创库
    hd.changeDatabase(database_now)
    tables = [i[0] for i in t["data"]]   # 获取当前库下数据表
    import datetime
    for i in tables:
        # 获取表结构
        fields = [(j[0], hd.hive_type[m.Field_TYPE[j[1]]]) for j in m.getTableInfo(i)]
        # print(m.Field_TYPE.values(), fields)  # 打印数据结构
        hd.createTable(i, fields)  # 创建表
        # 建立分区
        # TODO
        # 未写
        data = m.select("SELECT * FROM {}".format(i))["data"]
        # print(data)
        if data is not None:
            index = []  # 记录数据要加''的索引
            type_ = ["VARCHAR(256)", 'TIMESTAMP']  # 记录数据要加''的类型
            for j in range(len(fields)):
                if fields[j][1] in type_:
                    index.append(j)
            for j in range(len(data)):
                s = ""
                for k in range(len(data[0])):
                    if k in index:
                        if fields[k][1] == "TIMESTAMP":
                            s += " '" + str(data[j][k]).split(" ")[0] + "'"
                        else:
                            s += " '" + str(data[j][k]) + "'"
                    else:
                        s += " " + str(data[j][k])
                    if k != len(data[0]) - 1:
                        s += ","
                hd.insertDATA(i, data=s)
    m.close()
    hd.close()

if __name__ == "__main__":
    main()