import happybase
from iexfinance.stocks import Stock
from kafka import KafkaProducer
import os
import tweepy


twitter_api_key = os.environ.get('TWITTER_API_KEY')
twitter_api_secret = os.environ.get('TWITTER_API_SECRET_KEY')
twitter_token = os.environ.get('TWITTER_API_ACCESS_TOKEN')
twitter_token_secret = os.environ.get('TWITTER_API_ACCESS_TOKEN_SECRET')

kafka_twitter_topic = 'huarngpa_ingest_batch_twitter'
kafka_stock_topic = 'huarngpa_ingest_batch_stock'


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
    


def speed_layer_twitter(user):
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret)
    auth.set_access_token(twitter_token, twitter_token_secret)
    api = tweepy.API(auth)
    return api.user_timeline(screen_name=user)


def speed_layer_stock(ticker):
    stock = Stock(ticker, output_format='json')
    return stock.get_quote()
