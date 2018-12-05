create external table card_member(card_id BIGINT, member_id BIGINT, joindate STRING, last_update STRING, country STRING, 
city STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES 
("separatorChar" = ",","quoteChar"     = "\"") LOCATION 'hdfs:/user/hdfs/card_member'
