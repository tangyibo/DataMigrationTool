# -*- coding: UTF-8 -*-
from base_writer import WriterBase
import pymysql
from warnings import filterwarnings

filterwarnings("error", category=pymysql.Warning)


class WriterMysql(WriterBase):

    def __init__(self, host, port, dbname, username, password):
        WriterBase.__init__(self, host, port, dbname, username, password)

    def connect(self):
        self._connection = pymysql.connect(
            host=self.host,
            port=self.port,
            db=self.dbname,
            user=self.username,
            passwd=self.password,
            charset='utf8')

        # 该选项影响列为自增长的插入。在默认设置下，插入0或者null代表生成下一个自
        # 增长值。如果用户希望插入的值为0，而该列又是自增长的
        mysql_cursor = self._connection.cursor()
        try:
            mysql_cursor.execute("SET sql_mode='NO_AUTO_VALUE_ON_ZERO'")
            mysql_cursor.execute("set names 'utf8'")
        except pymysql.Warning as e:
            pass

        mysql_cursor.close()

    def close(self):
        self._connection.close()

    def drop_table(self, table_name):
        cursor = self._connection.cursor()

        try:
            drop_table_sql = "DROP TABLE IF EXISTS `%s`;" % table_name
            cursor.execute(drop_table_sql)
            self._connection.commit()
        except pymysql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(drop_table_sql)
            self._connection.commit()
        except Exception, e:
            self._connection.rollback()
            return False, e.message
        finally:
            cursor.close()

        return True, 'ok'

    def create_table(self, create_table_sql):
        cursor = self._connection.cursor()

        try:
            cursor.execute(create_table_sql)
            self._connection.commit()
        except pymysql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(create_table_sql)
            self._connection.commit()
        except Exception, e:
            self._connection.rollback()
            return False, e.message
        finally:
            cursor.close()

        return True, 'ok'

    def prepare_insert(self, table_name, column_names):
        question_marks = ",".join(["%s" for i in range(len(column_names))])
        sql_insert = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ",".join(column_names), question_marks)
        return sql_insert

    def insert_value(self, insert_sql, row):
        column_values = []
        for column_value in row:
            if column_value is None:
                column_values.append(None)
            else:
                column_values.append(pymysql.escape_string(str(column_value)))

        mysql_cursor = self._connection.cursor()
        try:
            mysql_cursor.execute(insert_sql, column_values)
            return True, 'ok'
        except pymysql.OperationalError, e:
            self.connect()
            return self.insert_value(insert_sql, row)
        except Exception, e:
            return False, e.message

        return False, 'error'

    def commit_operation(self):
        try:
            self._connection.commit()
            return True, 'ok'
        except pymysql.OperationalError, e:
            self.connect()
            return self.commit_operation()
        except Exception, e:
            return False, e.message

        return False, 'error'

