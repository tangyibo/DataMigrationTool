# -*- coding: UTF-8 -*-

class ReaderBase(object):

    # 构造函数
    def __init__(self, host, port, dbname, username, password):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._username = username
        self._password = password

        self._connection = None

    # 与数据库建立连接，设置self._connection的值
    def connect(self):
        pass

    # 关闭与数据库的连接
    def close(self):
        pass

    # 查询表内所有的数据
    def find_all(self, cursor, sql):
        pass

    # 获取表的创建语句
    def get_mysql_create_table_sql(self, curr_table_name, new_table_name=None):
        return False, "not implement", None

    # 装饰器 host
    @property
    def host(self):
        return self._host

    # 装饰器 port
    @property
    def port(self):
        return self._port

    # 装饰器 dbname
    @property
    def dbname(self):
        return self._dbname

    # 装饰器 username
    @property
    def username(self):
        return self._username

    # 装饰器 password
    @property
    def password(self):
        return self._password

    # 装饰器 connection
    @property
    def connection(self):
        return self._connection
