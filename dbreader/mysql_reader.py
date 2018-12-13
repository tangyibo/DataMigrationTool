# -*- coding: UTF-8 -*-
from base_reader import ReaderBase
import pymysql
from warnings import filterwarnings
filterwarnings("error",category=pymysql.Warning)

class ReaderMysql(ReaderBase):

    # 构造函数
    def __init__(self, host, port, dbname, username, password):
        ReaderBase.__init__(self, host, port, dbname, username, password)

    # 建立与mysql数据库的连接
    def connect(self):
        self._connection = pymysql.connect(
            host=self.host,
            port=self.port,
            db=self.dbname,
            user=self.username,
            passwd=self.password,
            charset='utf8')

    # 关闭与MySQL的连接
    def close(self):
        self._connection.close()

    # 查询表内所有的数据
    def find_all(self, cursor, sql):

        try:
            cursor.execute(sql)
        except pymysql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            return self.find_all(cursor,sql)
        except pymysql.Warning as e:
            pass
        except Exception, e:
            return False, e.message

        return True, cursor

    # 获取mysql的建表语句, 原理：利用MySQL的 show create table 语句获取
    def get_mysql_create_table_sql(self, curr_table_name, new_table_name=None, create_if_not_exist=False):
        mysql_cursor = self._connection.cursor()
        show_create_table_sql = "show create table %s " % curr_table_name
        try:
            mysql_cursor.execute(show_create_table_sql)
        except pymysql.OperationalError, e:
            self.connect()
            mysql_cursor = self._connection.cursor()
            mysql_cursor.execute(show_create_table_sql)
        except Exception, e:
            return False, e.message, []

        results = mysql_cursor.fetchone()
        if new_table_name is None:
            create_table_sql = results[1]
        else:
            create_table_sql = results[1].replace(curr_table_name, new_table_name)

        mysql_cursor.close()

        if create_if_not_exist is True:
            create_table_sql=create_table_sql.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS ')

        # remove the current time field
        create_table_sql=create_table_sql.replace('ON UPDATE CURRENT_TIMESTAMP',' ')

        column_names = []
        columns = self.__query_table_columns(curr_table_name)
        for col in columns:
            column_names.append(col[0])

        return True, create_table_sql, column_names

    # 获取表的列信息
    def __query_table_columns(self, table_name):
        cursor = self._connection.cursor()
        sql = "select column_name,data_type from information_schema.COLUMNS where TABLE_NAME='%s'" % table_name

        try:
            cursor.execute(sql)
        except pymysql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        columns = list(cursor.fetchall())
        cursor.close()
        return columns
