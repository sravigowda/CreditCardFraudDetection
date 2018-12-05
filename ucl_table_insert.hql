create table if not exists card_ucl (`card_id` string, `average` double, `stddev` double, `ucl` double) STORED AS TEXTFILE;

insert overwrite table card_ucl select card_id,avg(Amount), stddev_pop(Amount), avg(Amount)+3*stddev_pop(Amount) as UCL 
from ( select card_id, Amount, transaction_dt, rank() 
over(partition by card_id order by(unix_timestamp(transaction_dt,'dd-MM-yyyy HH:mm:ss'))DESC)as  rank 
from card_transactions_hive where status != 'FRAUD') t where  rank <= 10 GROUP BY card_id;
