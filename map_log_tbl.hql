-- Use default database
USE pos;

-- Map the transfer_logs table
CREATE EXTERNAL TABLE hive_transfer_logs (
    `row_key` STRING,
    `timestamp` STRING,
    `file_name` STRING,
    `status` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,log_data:timestamp,log_data:file_name,log_data:status"
)
TBLPROPERTIES (
    "hbase.table.name" = "transfer_logs"
);
