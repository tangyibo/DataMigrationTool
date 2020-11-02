数据迁移工具(Data Migration Tool,DMT)
===================================

此工具已经不再维护，新项目请见：https://gitee.com/inrgihc/dbswitch

一、功能简介
-----------
- 将远程数据库（包括Oracle、SqlServer、MySQL）中的表拉取迁移到本地
的MySQL数据库中，提供MySQL自动建表与数据（增量）导入功能。
- 基于MySQL的binlog日志计算在数据迁移同步过程中的增量数据；

二、实现逻辑
-----------
- 根据配置的源端数据库类型（Oracle、SqlServer、MySQL），相应的去读取库中的表结构信息，并生成
MySQL的建表语句，到目的端MySQL执行建表；
- 通过读取源端数据库表中的数据，将其增量推送到目的端库MySQL中；
- 利用MySQL的binlog机制接收在数据同步过程中的增量数据，以备业务使用。

数据迁移同步部分的逻辑代码如下：

 (1)为要同步的表增加一个专用扩展列T(时间类型)
 
 (2)在迁移同步每张表前，先记录下MySQL的当前时间（用于后面的delete同步）
 
 (3)利用MySQL的insert into ... on duplicate update ...语句，将源表中的数据灌如到目标表中。
 
 (4)利用扩展列T，同步需要删除的数据。

![逻辑图](https://github.com/tangyibo/DataMigrationTool/blob/master/doc/main.png)

说明：假设数据库中的表test有A,B两个字段，其中A为主键，T为本模块补充的时间类型字段。


三、MySQL的binlog配置
-----------------
在程序启动前，需要配置MySQL的my.cnf 配置文件如下：
```
server-id=1
log-bin=mysql-bin
binlog-format=ROW
binlog_row_image = full
```
四、编译使用
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
host=172.16.13.63
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

  说明：以下两个模块同时启动
  
 (1)启动数据迁移同步：
```buildoutcfg
python ./data_migration.py
```
 (2)启动数据增量处理：
```buildoutcfg
python ./data_increament.py
```


