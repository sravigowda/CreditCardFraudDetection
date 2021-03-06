/* Create an external table containing card_transactions. This is one time dump provided

create external table if not exists card_transactions(`card_id` string, `member_id` string, `Amount` Double, `postcode` INT, `pos_id` BIGINT,
`transaction_dt` string, `status` string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",") 
LOCATION 'hdfs:/user/hdfs/card_details' tblproperties("skip.header.line.count"="1");

/* Loding card transactions data into card_transactions table

LOAD DATA INPATH 'hdfs:/user/hdfs/card_transactions.csv' OVERWRITE INTO TABLE card_transactions;

/* Creating a Table in NOSQL Database. Following query will create a table in HBASE as well as HIVE

create table card_transactions_hive(`unique_id` bigint, `card_id` string, `member_id` string, `Amount` Double, 
`postcode` INT, `pos_id` BIGINT, `transaction_dt` string, `status` string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:card_id,cf2:member_id,cf3:Amount,cf4:postcode, 
cf5:pos_id, cf6: transaction_dt, cf7: status")
TBLPROPERTIES ("hbase.table.name" = "card_transactions_hbase");

/* Following query inserts data into the table created above, here we are using card_id and Amounts as part of the RowKey

insert overwrite table card_transactions_hive select crc32(card_transactions.card_id+card_transactions.Amount) as unique_id,
card_transactions.card_id, card_transactions.member_id, card_transactions.Amount, card_transactions.postcode, 
card_transactions.pos_id, card_transactions.transaction_dt, card_transactions.status from card_transactions;

/* Following query inserts data into card_transactions_hive table. Here we are using UUID as RowKey

create table card_transactions_hive(`unique_id` varchar(32), `card_id` string, `member_id` string, `Amount` Double, 
`postcode` INT, `pos_id` BIGINT, `transaction_dt` string, `status` string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:card_id,cf2:member_id,cf3:Amount,cf4:postcode, 
cf5:pos_id, cf6: transaction_dt, cf7: status") TBLPROPERTIES ("hbase.table.name" = "card_transactions_hbase"); 

insert overwrite table card_transactions_hive select regexp_replace(reflect("java.util.UUID","randomUUID"),'-',"") as unique_id,
card_transactions.card_id, card_transactions.member_id, card_transactions.Amount, card_transactions.postcode, 
card_transactions.pos_id, card_transactions.transaction_dt, card_transactions.status from card_transactions;

