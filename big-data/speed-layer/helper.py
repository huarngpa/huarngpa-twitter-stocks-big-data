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


def speed_layer_twitter(user):
    auth = tweepy.OAuthHandler(twitter_api_key, twitter_api_secret)
    auth.set_access_token(twitter_token, twitter_token_secret)
    api = tweepy.API(auth)
    return api.user_timeline(screen_name=user)


def speed_layer_stock(ticker):
    stock = Stock(ticker, output_format='json')
    return stock.get_quote()
