#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Date: 2018-12-03
# Author: tang
#
import sys, os
import datetime,time
from logger_file import *
from config_file import ConfigFile
from dbwriter import *
from dbreader import *


################
# 数据迁移工具类 #
################
class DataMigration:
    # 构造函数
    def __init__(self, filename):
        self.config = ConfigFile(filename)

        dbmapper = {
            "mysql": ReaderMysql,
            "oracle": ReaderOracle,
            "sqlserver": ReaderSqlserver
        }
        if not dbmapper.has_key(self.config.source_db_type):
            raise Exception("Unsupport database type :%s" % self.config.source_db_type)

        logger.info("server param: source database type is %s" % self.config.source_db_type)
        dbclass = dbmapper.get(self.config.source_db_type)
        self.db_reader = dbclass(
            host=self.config.source_db_host,
            port=self.config.source_db_port,
            dbname=self.config.source_db_dbname,
            username=self.config.source_db_user,
            password=self.config.source_db_passwd
        )
        self.db_writer = WriterMysql(
            host=self.config.destination_mysql_host,
            port=self.config.destination_mysql_port,
            dbname=self.config.destination_mysql_dbname,
            username=self.config.destination_mysql_user,
            password=self.config.destination_mysql_passwd
        )

        self.db_reader.connect()
        self.db_writer.connect()

    # 启动运行
    def run(self):
        success = True
        logger.info("running data migration ...")
        for src_table in self.config.mysql_table_map:
            starttime = datetime.datetime.now()
            ret=self.__handle_one_table(src_table, self.config.mysql_table_map[src_table], True, False)
            endtime = datetime.datetime.now()
            logger.info("migration table [%s] elipse %d(s)..." % (src_table, (endtime - starttime).seconds))
            success=success and ret

        return success

    # 运行完成
    def fini(self):
        self.db_reader.close()
        self.db_writer.close()

    # 处理一个表及其数据
    def __handle_one_table(self, src_table, dest_table=None, create_if_not_exist=True, drop_if_exists=False):
        logger.info("handle table:%s ..." % src_table)

        ret, create_table_sql, column_names = self.db_reader.get_mysql_create_table_sql(src_table, dest_table,
                                                                                        create_if_not_exist)
        if ret is False:
            logger.info("get create sql from source database failed,table:%s, error:%s" % (src_table, create_table_sql))
            return False

        logger.info("get create sql from source database is :\n%s " % create_table_sql)

        table_name = src_table
        if dest_table is not None:
            table_name = dest_table

        if drop_if_exists is True:
            self.db_writer.drop_table(table_name)

        ret, error = self.db_writer.create_table(create_table_sql)
        if ret is False:
            logger.error("error: %s" % error)
            return False

        reader_cursor = self.db_reader.connection.cursor()
        query_field_string = ",".join(["%s" % column_names[i] for i in range(len(column_names))])
        query_all_data_sql = "select %s from %s " % (query_field_string, src_table)
        logger.info("query all sql: %s" % query_all_data_sql)
        ret, reader_cursor = self.db_reader.find_all(reader_cursor, query_all_data_sql)
        if ret is False:
            logger.error("query all sql faild: %s" % reader_cursor)
            return False

        table_operator = self.db_writer.prepare_table_operator(table_name, column_names,drop_if_exists)
        logger.info("insert sql: %s" % table_operator.statement)
        success_insert_count=0
        table_row = reader_cursor.fetchone()
        while table_row is not None:
            ret, error = table_operator.append(table_row)
            if ret is False:
                logger.error("insert data sql faild,error %s" % error)
            else:
                success_insert_count = success_insert_count + 1
            if success_insert_count % 1000 is 0:
                logger.info("read table [%s] data count: %d,and insert data count: %d" % (
                    src_table,reader_cursor.rowcount, success_insert_count))
            table_row = reader_cursor.fetchone()

        ret, error = table_operator.commit()
        if ret is False:
            logger.error("insert data sql faild,error %s" % error)

        logger.info("query table [%s] data total count : %d,success insert %d " % (
        src_table, reader_cursor.rowcount, success_insert_count))

        reader_cursor.close()

        return True


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    logger.info("server start ...")

    file_name = '%s/config.ini' % os.path.dirname(os.path.realpath(__file__))
    migration = DataMigration(file_name)
    start = datetime.datetime.now()
    ret = migration.run()
    stop = datetime.datetime.now()
    migration.fini()
    logger.info("migration data elipse total %d(s)" % (stop - start).seconds)

    if not ret:
        logger.error("server run failed!")
        sys.exit(0)
    else:
        logger.info("server run success!")
        sys.exit(1)

    # shell 调用方法:
    #
    # python data_migration.py
    #
    # if [ $?==0 ];then
    #     echo 'success'
    # else
    #     echo 'error'
    # fi
