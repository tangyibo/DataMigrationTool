# -*- coding: UTF-8 -*-
from base_writer import WriterBase
import pymysql


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
        mysql_cursor.execute("SET sql_mode='NO_AUTO_VALUE_ON_ZERO'")
        mysql_cursor.close()

    def close(self):
        self._connection.close()

    def data_insert(self, table_name, row):
        WriterBase.data_insert(self, table_name, row)

        column_names = []
        column_values = []
        for column_name, column_value in row.items():
            if column_name == 'LINES':
                column_names.append('NUM_LINES')
            else:
                column_names.append(column_name)

            if column_value is None:
                column_values.append('NULL')
            else:
                column_values.append(pymysql.escape_string(str(column_value)))

        question_marks = ",".join(["%s" for i in range(len(column_names))])
        sql_insert = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ",".join(column_names), question_marks)
        mysql_cursor = self._connection.cursor()
        try:
            mysql_cursor.execute(sql_insert, column_values)
            self._connection.commit()
            return True, 'ok'
        except pymysql.OperationalError, e:
            self.connect()
            return self.data_insert(table_name, row)
        except Exception, e:
            return False, e.message

        return False, 'error'
