# -*- coding: UTF-8 -*-

import sys, os
import ConfigParser
import logging
import logging.handlers

__all__=["logger"]

# 加载日志配置
real_dir = os.path.dirname(os.path.realpath(__file__))
prog_name = os.path.basename(sys.argv[0])
name, suffix = os.path.splitext(prog_name)
try:
    os.mkdir("%s/log" % real_dir)
except WindowsError,e:
    pass

logfilename = "%s/log/log-%s.log" % (real_dir, name)
handler = logging.handlers.RotatingFileHandler(logfilename, maxBytes=1024 * 1024, backupCount=10)
formatter = logging.Formatter('%(asctime)s - [%(levelname)-8s]  - %(message)s  %(filename)s:%(lineno)s')
handler.setFormatter(formatter)
logger = logging.getLogger(logfilename)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)