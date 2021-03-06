SET hive.auto.convert.join=false;
create external table if not exists master_lookup (`card_id` string, `member_id` string, `joining_date` string, 
`purchase_date` string, `country` string, `city` string, `amount` bigint, `postcode` int, `transaction_date` string, 
`limit` bigint, `score` int) STORED AS TEXTFILE ;

insert overwrite table master_lookup select card_member.card_id, card_member.member_id, card_member.joindate, 
card_member.last_update, card_member.country, card_member.city, rank_trans.amount, rank_trans.postcode,
rank_trans.transaction_dt, card_ucl.ucl, member_score.score FROM card_member 
JOIN rank_trans ON (card_member.card_id = rank_trans.card_id) JOIN card_ucl ON (card_ucl.card_id = card_member.card_id) 
JOIN member_score ON (member_score.member_id = card_member.member_id);
