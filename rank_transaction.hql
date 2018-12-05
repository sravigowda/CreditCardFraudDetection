create table if not exists rank_trans (`card_id` STRING, `transaction_dt` STRING, `postcode` string, `amount` double) 
STORED as TEXTFILE;

insert overwrite table rank_trans select card_id,transaction_dt,postcode, Amount from 
( select card_id,transaction_dt,postcode, Amount, row_number() over 
(partition by card_id order by (unix_timestamp(transaction_dt,'dd-MM-yyyy HH:mm:ss'))DESC)as  rank 
from card_transactions_hive where status != 'FRAUD')t1 where  rank = 1;
