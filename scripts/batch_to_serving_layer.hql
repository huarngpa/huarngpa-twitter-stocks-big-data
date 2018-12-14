-- manual workaround for batch to serving layer


-- twitter

create external table if not exists huarngpa_batch_twitter (
  user_id string,
  count_tweets bigint,
  sum_retweets bigint,
  sum_favorited bigint,
  sum_sentiment double
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
  'hbase.columns.mapping' = ':key,weekly:count_tweets,weekly:sum_retweets,weekly:sum_favorited,weekly:sum_sentiment'
)
tblproperties ('hbase.table.name' = 'huarngpa_batch_twitter');

insert overwrite table huarngpa_batch_twitter
select
  C.user_id as user_id,
  C.count_tweets as count_tweets,
  C.sum_retweets as sum_retweets,
  C.sum_favorited as sum_favorited,
  C.sum_sentiment as sum_sentiment
from
  (select
     A.user_id as user_id,
     A.count_tweets as count_tweets,
     A.sum_retweets as sum_retweets,
     A.sum_favorited as sum_favorited,
     B.sum_sentiment as sum_sentiment
   from
     huarngpa_view_twitter_weekly A
   left join
     huarngpa_view_twitter_weekly_sentiment B
       on A.user_id = B.user_id) C
; 

-- stocks

create external table if not exists huarngpa_batch_stock (
  ticker string,
  count_trading_days bigint,
  sum_day_open double,
  max_day_high double,
  min_day_low double,
  sum_day_close double,
  sum_day_change double,
  sum_day_volume double
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
  'hbase.columns.mapping' = ':key,weekly:count_trading_days,weekly:sum_day_open,weekly:max_day_high,weekly:min_day_low,weekly:sum_day_close,weekly:sum_day_change,weekly:sum_day_volume'
)
tblproperties ('hbase.table.name' = 'huarngpa_batch_stock');

insert overwrite table huarngpa_batch_stock
select
  ticker,
  count_trading_days,
  sum_day_open,
  max_day_high,
  min_day_low,
  sum_day_close,
  sum_day_change,
  sum_day_volume
from
  huarngpa_view_stock_weekly;
