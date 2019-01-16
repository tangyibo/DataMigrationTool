#!/usr/bin/python
# -*- coding: UTF-8 -*-
# Date: 2019-01-15
# Author: tang
# Document URL:
# https://python-mysql-replication.readthedocs.io/en/latest/_modules/pymysqlreplication/binlogstream.html
#
##################################################
# 基于MySQL的binlog计算数据迁移同步过程中的增量数据
# 即：insert/update/delete操作
# ------------------------------------------------
# 说明：在程序启动前，需要配置MySQL的my.cnf 配置文件如下：
##################################################
# server-id=1
# log-bin=mysql-bin
# binlog-format=ROW
# binlog_row_image = full
##################################################

import sys, os
import json
from datetime import datetime, date
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from logger_file import *
from config_file import ConfigFile


################
# 数据增量工具类 #
################
class BinlogIncrement(object):
    magic_field_name = 'magic_time'

    # 构造函数
    def __init__(self, filename):
        self.config = ConfigFile(filename)
        self.mysql_settings = {
            'host': self.config.destination_mysql_host,
            'port': self.config.destination_mysql_port,
            'user': self.config.destination_mysql_user,
            'passwd': self.config.destination_mysql_passwd
        }

    # 利用binlog计算增量
    def run(self, callback):
        stream = BinLogStreamReader(
            connection_settings=self.mysql_settings,
            blocking=True,
            server_id=100,
            resume_stream=True,
            only_schemas=self.config.destination_mysql_dbname,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
        )

        logger.info("running data increment ...")

        for binlogevent in stream:
            for row in binlogevent.rows:
                event = {"schema": binlogevent.schema, "table": binlogevent.table}

                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"

                    new_data = row["after_values"]
                    old_data = row["before_values"]

                    old = {}
                    for key, val in new_data.items():
                        if val != old_data.get(key):
                            old[key] = old_data[key]

                    # 对于binlog中的update操作，如果只有magic_field_name字段被修改，那说明
                    # 这条记录没有变化，所以忽略这个binlog数据；否则才为实际的update操作。
                    if BinlogIncrement.magic_field_name in old and len(old) == 1:
                        continue

                    event['data'] = new_data
                    event['old'] = old
                    event = dict(event.items())
                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]
                    event = dict(event.items())

                callback(event)

        stream.close()


##########################################################

# 对于dict中的日期字段进行处理，以便能够将dict转换成JSON格式
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


# 增量的处理，这里只是简单的打印输出
def handle_data_increment(data):
    print json.dumps(data, cls=ComplexEncoder)
    sys.stdout.flush()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    logger.info("server start ...")
    file_name = '%s/config.ini' % os.path.dirname(os.path.realpath(__file__))
    binlog = BinlogIncrement(file_name)
    binlog.run(handle_data_increment)
    logger.info("server run stop ...")
