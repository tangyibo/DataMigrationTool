# -*- coding: UTF-8 -*-
from base_reader import ReaderBase
import cx_Oracle


class ReaderOracle(ReaderBase):

    # 构造函数
    def __init__(self, host, port, dbname, username, password):
        ReaderBase.__init__(self, host, port, dbname, username, password)

    # 建立与oracle数据库的连接
    def connect(self):
        tns = cx_Oracle.makedsn(self.host, self.port, self.dbname)
        self._connection = cx_Oracle.connect(self.username, self.username, tns)

    def close(self):
        self._connection.close()

    def find_all(self, cursor, sql):

        try:
            cursor.execute(sql)
        except cx_Oracle.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            return False, e.message

        return True, cursor

    # 获取oracle的建表语句,原理：利用Oracle的 SELECT * FROM table where rownum<1 语句获取列名信息
    def get_mysql_create_table_sql(self, curr_table_name, new_table_name=None):
        oracle_cursor = self._connection.cursor()

        sql = "SELECT * FROM %s where rownum<1" % curr_table_name
        try:
            oracle_cursor.execute(sql)
        except cx_Oracle.OperationalError, e:
            self.connect()
            oracle_cursor = self._connection.cursor()
            oracle_cursor.execute(sql)
        except Exception, e:
            return False, e.message, []

        table_metadata = []
        columns_names = []
        # "The description is a list of 7-item tuples where each tuple
        # consists of a column name, column type, display size, internal size,
        # precision, scale and whether null is possible."
        for column in oracle_cursor.description:
            columns_names.append(column[0])
            table_metadata.append({
                'name': column[0],
                'type': column[1],
                'display_size': column[2],
                'internal_size': column[3],
                'precision': column[4],
                'scale': column[5],
                'nullable': column[6],
            })

        oracle_cursor.close()

        try:
            # 获取主键列信息
            primary_key_column = self.__query_table_primary_key(curr_table_name)
        except Exception, e:
            return False, e.message, []

        table_name = curr_table_name
        if new_table_name is not None:
            table_name = new_table_name
        create_table_sql = "CREATE TABLE %s (\n" % (table_name,)
        column_definitions = []

        for column in table_metadata:
            # 'LINES' is a MySQL reserved keyword
            column_name = column['name']
            if column_name == "LINES":
                column_name = "NUM_LINES"

            if column['type'] == cx_Oracle.NUMBER:
                # column_type = "DECIMAL(%s, %s)" % (column['precision'], column['scale'])
                column_type = "BIGINT"
            elif column['type'] == cx_Oracle.STRING:
                if column['internal_size'] > 256:
                    column_type = "TEXT"
                else:
                    column_type = "VARCHAR(%s)" % (column['internal_size'],)
            elif column['type'] == cx_Oracle.DATETIME:
                column_type = "DATETIME"
            elif column['type'] == cx_Oracle.TIMESTAMP:
                column_type = "TIMESTAMP"
            elif column['type'] == cx_Oracle.FIXED_CHAR:
                column_type = "CHAR(%s)" % (column['internal_size'],)
            else: # cx_Oracle.CLOB or cx_Oracle.BLOB
                raise Exception("No mapping for column type %s" % (column['type'],))

            if column['nullable'] == 1:
                nullable = "null"
            else:
                nullable = "not null"

            column_definitions.append("%s %s %s" % (column_name, column_type, nullable))

        create_table_sql += ",\n".join(column_definitions)

        if primary_key_column:
            create_table_sql += ',\nPRIMARY KEY (`%s`)' % primary_key_column
        create_table_sql += "\n)ENGINE=InnoDB DEFAULT CHARACTER SET = utf8;"

        return True, create_table_sql, columns_names

    # 获取表的主键列信息
    def __query_table_primary_key(self, table_name):
        oracle_cursor = self._connection.cursor()

        sql = "SELECT COLUMN_NAME FROM user_cons_columns WHERE	constraint_name = \
        (SELECT constraint_name FROM user_constraints WHERE table_name = '%s' AND \
        constraint_type = 'P') " % table_name
        try:
            oracle_cursor.execute(sql)
        except cx_Oracle.OperationalError, e:
            self.connect()
            oracle_cursor = self._connection.cursor()
            oracle_cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        r = oracle_cursor.fetchall()
        oracle_cursor.close()
        return r[0][0] if r else None
