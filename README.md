# flink-sql-hdfs-connector
支持根据数据的事件时间落到对应的分区目录
# 分支说明
master分支不放代码，分支对应适配相同版本的flink，例如分支 flink-1.10 就仅在flink 1.10 版本上测试通过
# 使用方法
下载代码编译
```shell
cd flink-sql-hdfs-connector
mvn clean package
ll target/flink-sql-hdfs-connector-1.10-shaded.jar
```
复制 target/flink-sql-hdfs-connector-1.10-shaded.jar 到 flink安装目录的lib下即可使用
# sql样例
```sql
CREATE TABLE hdfs_file_sink (
  `id`          bigint,
  `policy_id`   INT,
  `policy_no`   varchar,
  `plan_code`   int ,
  `gmt_created` TIMESTAMP(3),
  `is_deleted`  varchar ,
  `price`       DECIMAL(38, 18),
  `rate`        float ,
  `large_rate`  double
) WITH (
  'connector.type'='hdfs',
  'connector.write.base-path'='hdfs://nameservice-ha/user/xushanshan/tmp',  -- 集群的目录 需要写全路径包括文件系统类型和地址，例如 hdfs://nameservice-ha/
  'connector.write.bucket-assigner-format'='yyyy-MM-dd-HH-mm', -- 日期分区目录格式，不要加空格或特殊字符
  'connector.write.field-delimiter'='\u0001',             -- 列分隔符
  'connector.write.event-field'='gmt_created'             -- 事件时间列  
);
```
