from datetime import datetime
import happybase
from iexfinance.stocks import Stock
from kafka import KafkaProducer
import os
from records import *
import tweepy


twitter_api_key = os.environ.get('TWITTER_API_KEY')
twitter_api_secret = os.environ.get('TWITTER_API_SECRET_KEY')
twitter_token = os.environ.get('TWITTER_API_ACCESS_TOKEN')
twitter_token_secret = os.environ.get('TWITTER_API_ACCESS_TOKEN_SECRET')

kafka_twitter_topic = 'huarngpa_ingest_batch_twitter'
kafka_stock_topic = 'huarngpa_ingest_batch_stock'
kafka_host = os.environ.get('API_KAFKA_HOST') + ':' +\
             os.environ.get('API_KAFKA_PORT')


def create_hbase_tables(hbase_connection):
    c = hbase_connection
    c.create_table('huarngpa_batch_twitter', {'all': dict(), 'weekly': dict()})
    c.create_table('huarngpa_batch_stock', {'all': dict(), 'weekly': dict()})
    c.create_table('huarngpa_batch_twitter_stock', {'linreg': dict()})
    c.create_table('huarngpa_speed_twitter', {'all': dict(), 'weekly': dict()})
    c.create_table('huarngpa_speed_stock', {'all': dict(), 'weekly': dict()})
    c.create_table('huarngpa_speed_twitter_stock', {'linreg': dict()})


def get_twitter_keys(hbase_connection):
    table = hbase_connection.table('huarngpa_speed_twitter')
    user_list = []
    for key, data in table.scan():
        user_list.append(key)
    return set(user_list)


def get_stock_keys(hbase_connection):
    table = hbase_connection.table('huarngpa_speed_stock')
    ticker_list = []
    for key, data in table.scan():
        ticker_list.append(key)
    return set(ticker_list)


def speed_layer_twitter_data(user):
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret)
    auth.set_access_token(twitter_token, twitter_token_secret)
    api = tweepy.API(auth)
    return api.user_timeline(user_id=user)


def speed_layer_stock_data(ticker):
    stock = Stock(ticker, output_format='json')
    return stock.get_quote()


def convert_statuses_to_objects(statuses):
    objects = []
    for status in statuses:
        t = convert_to_tweet_object(status)
        objects.append(t)
    return objects


def convert_to_tweet_object(status):
    t = Tweet(status.created_at,
              status.id_str,
              status.text,
              status.retweet_count,
              status.favorite_count,
              status.user.id_str)
    return t


def write_to_batch_layer_twitter(objects):
    for obj in objects:
        kafka_producer = KafkaProducer(bootstrap_servers=kafka_host)
        kafka_producer.send(kafka_twitter_topic,
                            str(obj).encode('utf-8'))


def write_to_serving_layer_twitter(hbase_connection, user_id, objects):
    min_date = None
    max_date = None
    count_tweets = 0
    sum_retweets = 0
    sum_favorited = 0
    for obj in objects:
        d = datetime.strptime(obj.created_at_day, '%Y-%m-%d')
        if min_date == None or d < min_date:
            min_date = d
        if max_date == None or d > max_date:
            max_date = d
        count_tweets += 1
        sum_retweets += obj.retweeted_count
        sum_favorited += obj.favorite_count
    try:
        min_date = min_date.isoformat()[:10]
        max_date = max_date.isoformat()[:10]
        table = hbase_connection.table('huarngpa_speed_twitter')
        table.put(user_id.encode('utf-8'), {
            b'all:min_date': min_date.encode('utf-8'),
            b'all:max_date': max_date.encode('utf-8'),
            b'all:count_tweets': str(count_tweets).encode('utf-8'),
            b'all:sum_retweets': str(sum_retweets).encode('utf-8'),
            b'all:sum_favorited': str(sum_favorited).encode('utf-8'),
            b'weekly:min_date': min_date.encode('utf-8'),
            b'weekly:max_date': max_date.encode('utf-8'),
            b'weekly:count_tweets': str(count_tweets).encode('utf-8'),
            b'weekly:sum_retweets': str(sum_retweets).encode('utf-8'),
            b'weekly:sum_favorited': str(sum_favorited).encode('utf-8'),
        })
    except Exception as e:
        print(e)


def convert_to_stock_object(data):
    h = StockHistory(data['symbol'].lower(),
                     datetime.fromtimestamp(data['closeTime']/1000).isoformat()[:10],
                     data['open'],
                     data['high'],
                     data['low'],
                     data['close'],
                     data['latestVolume'])
    return h


def write_to_batch_layer_stock(obj):
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_host)
    kafka_producer.send(kafka_stock_topic,
                        str(obj).encode('utf-8'))


def write_to_serving_layer_stock(hbase_connection, obj):
    try:
        table = hbase_connection.table('huarngpa_speed_stock')
        table.put(obj.ticker.encode('utf-8'), {
            b'all:count_trading_days': str(1).encode('utf-8'),
            b'all:sum_day_open': str(obj.day_open).encode('utf-8'),
            b'all:max_day_high': str(obj.day_high).encode('utf-8'),
            b'all:min_day_low': str(obj.day_low).encode('utf-8'),
            b'all:sum_day_close': str(obj.day_close).encode('utf-8'),
            b'all:sum_day_change': str(obj.day_close - obj.day_open).encode('utf-8'),
            b'all:sum_day_volume': str(obj.day_volume).encode('utf-8'),
            b'weekly:count_trading_days': str(1).encode('utf-8'),
            b'weekly:sum_day_open': str(obj.day_open).encode('utf-8'),
            b'weekly:max_day_high': str(obj.day_high).encode('utf-8'),
            b'weekly:min_day_low': str(obj.day_low).encode('utf-8'),
            b'weekly:sum_day_close': str(obj.day_close).encode('utf-8'),
            b'weekly:sum_day_change': str(obj.day_close - obj.day_open).encode('utf-8'),
            b'weekly:sum_day_volume': str(obj.day_volume).encode('utf-8'),
        })
    except Exception as e:
        print(e)
