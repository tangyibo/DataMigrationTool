# -*- coding: UTF-8 -*-


class WriterBase(object):

    # 构造函数
    def __init__(self, host, port, dbname, username, password):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._username = username
        self._password = password

        self._connection = None

    def connect(self):
        pass

    def close(self):
        pass

    def data_insert(self, table_name, row):
        if not isinstance(row, dict):
            raise Exception('Invalid param')

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
