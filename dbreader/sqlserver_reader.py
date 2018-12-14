# -*- coding: UTF-8 -*-
from base_reader import ReaderBase
import pymssql
import re


class ColumnDesc:
    TABLE_CATALOG = 0
    TABLE_SCHEMA = 1
    TABLE_NAME = 2
    COLUMN_NAME = 3
    ORDINAL_POSITION = 4
    COLUMN_DEFAULT = 5
    IS_NULLABLE = 6
    DATA_TYPE = 7
    CHARACTER_MAXIMUM_LENGTH = 8
    CHARACTER_OCTET_LENGTH = 9
    NUMERIC_PRECISION = 10
    NUMERIC_PRECISION_RADIX = 11
    NUMERIC_SCALE = 12
    DATETIME_PRECISION = 13
    CHARACTER_SET_CATALOG = 14
    CHARACTER_SET_SCHEMA = 15
    CHARACTER_SET_NAME = 16
    COLLATION_CATALOG = 17
    COLLATION_SCHEMA = 18
    COLLATION_NAME = 19
    DOMAIN_CATALOG = 20
    DOMAIN_SCHEMA = 21
    DOMAIN_NAME = 22
    IS_IDENTITY = 23


def handle_decimal_type(column_desc):
    precision = column_desc[ColumnDesc.NUMERIC_PRECISION]
    scale = column_desc[ColumnDesc.NUMERIC_SCALE]
    return "DECIMAL(%s, %s)" % (precision, scale)


def handle_char_type(column_desc):
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if length <= 255:
        return "CHAR(%s)" % length
    else:
        return "LONGTEXT"


def handle_text_type(column_desc):
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if 0 <= length <= 65535:
        return "VARCHAR(%s)" % length
    elif 0 <= length <= 166777215:
        return "MEDIUMTEXT"
    else:
        return "LONGTEXT"


def handle_blob_type(column_desc):
    data_type = column_desc[ColumnDesc.DATA_TYPE].upper()
    length = column_desc[ColumnDesc.CHARACTER_MAXIMUM_LENGTH]
    if data_type == "BINARY" and length <= 255:
        return "BINARY(%s)" % length
    elif data_type == "VARBINARY" and 0 <= length <= 65535:
        return "VARBINARY(%s)" % length
    elif 0 <= length <= 65535:
        return "BLOB"
    elif 0 <= length <= 166777215:
        return "MEDIUMBLOB"
    else:
        return "LONGBLOB"


TYPES_MAP = {
    'INT': 'INT',
    'TINYINT': 'TINYINT',
    'SMALLINT': 'SMALLINT',
    'BIGINT': 'BIGINT',
    'BIT': 'TINYINT(1)',
    'FLOAT': 'FLOAT',
    'REAL': 'FLOAT',
    'NUMERIC': handle_decimal_type,
    'DECIMAL': handle_decimal_type,
    'MONEY': handle_decimal_type,
    'SMALLMONEY': handle_decimal_type,
    'CHAR': handle_char_type,
    'NCHAR': handle_char_type,
    'VARCHAR': handle_text_type,
    'NVARCHAR': handle_text_type,
    'DATE': 'DATE',
    'DATETIME': 'DATETIME',
    'DATETIME2': 'DATETIME',
    'SMALLDATETIME': 'DATETIME',
    'DATETIMEOFFSET': 'DATETIME',
    'TIME': 'TIME',
    'TIMESTAMP': 'TIMESTAMP',
    'ROWVERSION': 'TIMESTAMP',
    'BINARY': handle_blob_type,
    'VARBINARY': handle_blob_type,
    'TEXT': handle_text_type,
    'NTEXT': handle_text_type,
    'IMAGE': handle_blob_type,
    'SQL_VARIANT': handle_blob_type,
    'TABLE': handle_blob_type,
    'HIERARCHYID': handle_blob_type,
    'UNIQUEIDENTIFIER': 'VARCHAR(64)',
    'SYSNAME': 'VARCHAR(160)',
    'XML': 'TEXT'
}


def get_column_type(column_desc):
    source_type = column_desc[ColumnDesc.DATA_TYPE].upper()
    target_type = TYPES_MAP.get(source_type)
    if target_type is None:
        return None
    elif isinstance(target_type, basestring):
        return target_type
    else:
        return target_type(column_desc)


def convert_column_default(col):
    default_value = col[ColumnDesc.COLUMN_DEFAULT]
    if default_value is None:
        return ''
    if default_value.startswith('((') and default_value.endswith('))'):
        default_value = default_value[2:-2]
    elif default_value.startswith('(') and default_value.endswith(')'):
        default_value = default_value[1:-1]

    if '(' in default_value and ')' in default_value:
        default_value = None
    elif default_value.startswith('CREATE'):
        default_value = None
    return ' DEFAULT %s' % default_value if default_value else ''


class ReaderSqlserver(ReaderBase):

    # 构造函数
    def __init__(self, host, port, dbname, username, password,magic_field_name):
        ReaderBase.__init__(self, host, port, dbname, username, password,magic_field_name)

    # 建立与SQLServer数据库的连接
    def connect(self):
        params = {'server': self.host, 'port': self.port, 'database': self.dbname,
                  'user': self.username, 'password': self.password, }
        self._connection = pymssql.connect(timeout=90, **params)

    # 关闭与SQLServer的连接
    def close(self):
        pass

    # 查询表内所有的数据
    def find_all(self, cursor, sql):

        try:
            cursor.execute(sql)
        except pymssql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            return False, e.message

        return True, cursor

    # 获取SQLServer的建表语句,原理：利用SQLServer的三个SQL获取表的列、主键、索引信息，然后生成MySQL的建表语句
    def get_mysql_create_table_sql(self, curr_table_name, new_table_name=None, create_if_not_exist=False):

        try:
            # 获取列信息
            columns = self.__query_table_columns(curr_table_name)

            # 获取主键列信息
            primary_key_column = self.__query_table_primary_key(curr_table_name)

            # 获取索引信息
            indexes = self.__query_table_indexes(curr_table_name)
        except Exception, e:
            return False, e.message, []

        indexes = indexes or []

        ######################
        # 生成创建表的SQL语句
        ######################

        columns = sorted(columns, key=lambda x: x[ColumnDesc.ORDINAL_POSITION])

        table_name = columns[0][ColumnDesc.TABLE_NAME]
        if new_table_name is not None:
            table_name = new_table_name

        cols = []
        columns_names = []
        auto_increment_column = None
        for col in columns:
            columns_names.append(col[ColumnDesc.COLUMN_NAME])
            cols.append("`%s` %s %s%s" % (col[ColumnDesc.COLUMN_NAME],
                                            get_column_type(col),
                                            convert_column_default(col),
                                            " NOT NULL" if col[ColumnDesc.IS_NULLABLE] == 'NO' else ''))
            auto_increment_column = col[ColumnDesc.COLUMN_NAME] if col[
                ColumnDesc.IS_IDENTITY] else auto_increment_column

        cols.append("%s timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'" % self.magic_field_name)

        if primary_key_column:
            primary_key_column_fields = ",".join(["`%s`" % i for i in primary_key_column])
            cols.append('PRIMARY KEY (%s)' % primary_key_column_fields)

        for index in indexes:
            unique = 'UNIQUE' if 'unique' in index[1].lower() else ''
            cols.append('%s KEY `%s` (%s)' % (unique, index[0][:64], re.sub("\([+-]+\)", "", index[2])))

        if create_if_not_exist:
            create_table_sql = 'CREATE TABLE IF NOT EXISTS `%s` (\n%s) ENGINE=InnoDB DEFAULT CHARSET=utf8' % (
                table_name, ',\n'.join(cols))
        else:
            create_table_sql = 'CREATE TABLE `%s` (\n%s) ENGINE=InnoDB DEFAULT CHARSET=utf8' % (
                table_name, ',\n'.join(cols))

        return True, create_table_sql, columns_names

    # 获取表的列信息
    def __query_table_columns(self, table_name):
        cursor = self._connection.cursor()
        sql = "select *, COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IS_IDENTITY from information_schema.COLUMNS where TABLE_NAME='%s'" % table_name

        try:
            cursor.execute(sql)
        except pymssql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        columns = list(cursor.fetchall())
        cursor.close()
        return columns

    # 获取表的主键列信息
    def __query_table_primary_key(self, table_name):
        cursor = self._connection.cursor()
        sql = "select CONSTRAINT_NAME from information_schema.TABLE_CONSTRAINTS where TABLE_NAME='%s' and CONSTRAINT_TYPE='PRIMARY KEY'" \
              % table_name

        try:
            cursor.execute(sql)
        except pymssql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        r = cursor.fetchall()
        cursor.close()
        if not r:
            return None

        constraint_name = r[0][0]
        sql = "select COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where TABLE_NAME='%s' and CONSTRAINT_NAME='%s'" \
              % (table_name, constraint_name)

        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
        except pymssql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        r = cursor.fetchall()
        cursor.close()

        ret = []
        if r:
            for item in r:
                ret.append(item[0])

        return ret

    # 获取表的索引信息
    def __query_table_indexes(self, table_name):
        cursor = self._connection.cursor()
        sql = "sp_helpindex '%s'" % table_name

        try:
            cursor.execute(sql)
        except pymssql.OperationalError, e:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute(sql)
        except Exception, e:
            raise Exception(e.message)

        indexes = list(cursor.fetchall()) if cursor.description else []
        cursor.close()
        return indexes
