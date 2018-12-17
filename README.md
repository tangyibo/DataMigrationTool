数据迁移工具(Data Migration Tool,DMT)
===================================

一、功能简介
-----------
将远程数据库（包括Oracle、SqlServer、MySQL）中的表拉取迁移到本地
的MySQL数据库中，提供MySQL自动建表与数据导入功能。

二、基础要求
------------
 - 表必须有主键
 - 支持insert/update/delete

三、实现逻辑
-----------
根据配置的源端数据库类型（Oracle、SqlServer、MySQL），相应的去读取库中的表结构信息，并生成
MySQL的建表语句，到目的端MySQL执行，并进行数据由源库迁移到目的库MySQL中,提供异构数据同步功能。

四、具有功能
------------
 - 异构数据库到MySQL的表结构建立；
 - 异构数据库到MySQL的数据同步；

五、编译使用
-----------

1、依赖安装
```
  pip install -r requirements.txt
```
2、修改配置文件
```
[source]
;数据库类型,支持的类型包括：oracle,sqlserver,mysql
type=sqlserver
;源端数据库ip地址
host=127.10.5.63
;源端数据库端口
port=1433
;源端数据库用户
user=ei
;源端数据库用户密码
passwd=fssaa
;源端数据库名称
dbname=edu
;源端表名列表,表名间用英文逗号分隔
tbname=tbClass,tbCourseDomain,tbRoom

[destination]
;目标数据库类型必须为mysql,即type=mysql
;目的mysql 数据库ip地址
host=127.0.0.1
;目的mysql 数据库端口
port=3306
;目的mysql 数据库用户
user=tangyibo
;目的mysql 数据库用户密码
passwd=tangyibo
;目的mysql 数据库名称
dbname=tangyb
;目的mysql 表名列表,表名间用英文逗号分隔
tbname=tb_class,tb_course_domain,tb_room
```
3、启动运行
```
python ./data_migration.py
```

