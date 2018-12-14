-- manual workaround for batch to serving layer


-- twitter

create external table if not exists huarngpa_batch_twitter (
  user_id string,
  count_tweets bigint,
  sum_retweets bigint,
  sum_favorited bigint
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
  'hbase.columns.mapping' = ':key,weekly:count_tweets,weekly:sum_retweets,weekly:sum_favorited'
)
tblproperties ('hbase.table.name' = 'huarngpa_batch_twitter');

insert overwrite table huarngpa_batch_twitter
select
  user_id,
  count_tweets,
  sum_retweets,
  sum_favorited
from
  huarngpa_view_twitter_weekly;
 

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
