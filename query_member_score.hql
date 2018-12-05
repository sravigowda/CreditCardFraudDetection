create external table if not exists member_score(member_id BIGINT, score INT ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH 
SERDEPROPERTIES ("separatorChar" = ",","quoteChar"     = "\"") LOCATION 'hdfs:/user/hdfs/member_score';
