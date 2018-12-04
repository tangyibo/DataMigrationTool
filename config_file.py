# -*- coding: UTF-8 -*-
import os
import ConfigParser

class ConfigFile:

    def __init__(self,filename):
        self.__cf = ConfigParser.ConfigParser()
        self._filename=filename
        self.__parse()

    # 装饰器 filename
    @property
    def filename(self):
        return self._filename

    def __parse(self):
        if not os.path.exists:
            raise RuntimeError('file not exist:%s' % self._filename)

        self.__cf.read(self._filename)

        self.source_db_type=self.__cf.get("source", "type")
        self.source_db_host = self.__cf.get("source", "host")
        self.source_db_port = int(self.__cf.get("source", "port"))
        self.source_db_user = self.__cf.get("source", "user")
        self.source_db_passwd = self.__cf.get("source", "passwd")
        self.source_db_dbname = self.__cf.get("source", "dbname")

        self.destination_mysql_host = self.__cf.get("destination", "host")
        self.destination_mysql_port = int(self.__cf.get("destination", "port"))
        self.destination_mysql_user = self.__cf.get("destination", "user")
        self.destination_mysql_passwd = self.__cf.get("destination", "passwd")
        self.destination_mysql_dbname = self.__cf.get("destination", "dbname")

        source_db_tables_list = self.__cf.get("source", "tbname").split(",")
        destination_mysql_tables_list = self.__cf.get("destination", "tbname").split(",")

        src_len = len(source_db_tables_list)
        dest_len = len(destination_mysql_tables_list)
        if src_len != dest_len:
            raise RuntimeError('source table list count(%d) not equal destination table list count(%d)',
                               (src_len, dest_len))

        self.mysql_table_map = dict(zip(source_db_tables_list, destination_mysql_tables_list))
