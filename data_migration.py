#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Date: 2018-12-03
# Author: tang
#
import sys, os
import pymysql
from logger_file import *
from config_file import ConfigFile
from dbwriter import *
from dbreader import *

'''
 数据迁移类
'''


class DataMigration:

    def __init__(self, filename):
        self.config = ConfigFile(filename)

        dbmapper = {
            "mysql": ReaderMysql,
            "oracle": ReaderOracle,
            "sqlserver": ReaderSqlserver
        }
        if not dbmapper.has_key(self.config.source_db_type):
            raise Exception("Unsupport database type :%s" % self.config.source_db_type)

        logger.info("server param: source db type=%s" % self.config.source_db_type)
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

    def run(self):
        success = True
        logger.info("running data migration ...")
        for src_table in self.config.mysql_table_map:
            if not self.__handle_one_table(src_table, self.config.mysql_table_map[src_table], True):
                return False

        return success

    def fini(self):
        self.db_reader.close()
        self.db_writer.close()

    def __handle_one_table(self, src_table, dest_table=None, drop_if_exists=False):
        logger.info("handle table:%s ..." % src_table)
        ret, create_table_sql, column_names = self.db_reader.get_mysql_create_table_sql(src_table, dest_table)
        if ret is False:
            logger.info("get create sql from source database failed,table:%s, error:%s" % (src_table, create_table_sql))
            return False

        table_name = src_table
        if dest_table is not None:
            table_name = dest_table

        writer_cursor = self.db_writer.connection.cursor()

        try:
            if drop_if_exists:
                drop_table_sql = "DROP TABLE IF EXISTS %s;" % table_name
                writer_cursor.execute(drop_table_sql)
                logger.info("Writer execute drop table sql: %s" % drop_table_sql)

            writer_cursor.execute(create_table_sql)
            logger.info("Writer execute create table sql: \n%s" % create_table_sql)
            self.db_writer.connection.commit()
        except pymysql.OperationalError, e:
            logger.info("Reconnect writer databse ...")
            self.db_writer.connect()
            writer_cursor = self.db_writer.connection.cursor()
            if drop_if_exists:
                writer_cursor.execute(drop_table_sql)
            writer_cursor.execute(create_table_sql)
            self.db_writer.connection.commit()
        except Exception, e:
            self.db_writer.connection.rollback()
            logger.error("error: %s" % e.message)
            return False

        reader_cursor = self.db_reader.connection.cursor()
        query_field_string = ",".join(["%s" % column_names[i] for i in range(len(column_names))])
        query_all_data_sql = "select %s from %s " % (query_field_string, src_table)
        logger.info("query all sql: %s" % query_all_data_sql)
        ret, reader_cursor = self.db_reader.find_all(reader_cursor, query_all_data_sql)
        if ret is False:
            logger.error("query all sql faild: %s" % reader_cursor)
            return False

        query_row_count = 0
        insert_row_count = 0
        while True:
            table_row = reader_cursor.fetchone()
            if table_row is None:
                break
            else:
                row = {}
                for i in range(len(column_names)):
                    row[column_names[i]] = table_row[i]

                query_row_count = query_row_count + 1
                ret, error = self.db_writer.data_insert(table_name, row)
                if ret is False:
                    logger.error("insert data sql faild,error %s" % error)
                else:
                    insert_row_count = insert_row_count + 1

        logger.info("query table [%s] data total count : %d , insert table [%s] data total count:%d " \
                    % (src_table, query_row_count, table_name, insert_row_count))
        reader_cursor.close()

        return True


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    logger.info("server start ...")

    ret = False
    try:
        file_name = '%s/config.ini' % os.path.dirname(os.path.realpath(__file__))
        migration = DataMigration(file_name)
        ret = migration.run()
        migration.fini()
    except Exception, e:
        logger.error("server run error:%s" % e.message)

    if not ret:
        logger.error("server run failed!")
        sys.exit(0)
    else:
        logger.info("server run success!")
        sys.exit(1)

    # shell 调用方法:
    #
    # python hello.py
    #
    # if [ $?==0 ];then
    #     exit
    # else
    #     python world.py
    # fi
