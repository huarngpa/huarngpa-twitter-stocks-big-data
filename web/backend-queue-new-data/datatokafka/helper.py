''' Helper classes or functions for the application. '''

import happybase
from multiprocessing import Process
import os
from threading import Thread
import tweepy
from datatokafka.models import (db,
                                StockHistory, 
                                TwitterUser,)


class Joiner(Thread):
    ''' Helps us manage processes we open with the multiprocessing
        library. Orderly execution and queue management for jobs. '''

    def __init__(self, q):
        super(Joiner, self).__init__()
        self.__q = q

    def run(self):
        while True:
            if len(self.__q) == 0:
                return
            child = self.__q.pop(0)
            child.join()


def start_retrieval_process(helper, arg):
    p = Process(target=helper.get, args=(arg,))
    p.start()
    manager = Joiner([p])
    manager.start()


def write_stock_history_request(ticker):
    h = StockHistory()
    h.ticker = ticker
    db.session.add(h)
    db.session.commit()


def bootstrap_tweepy():
    api_key = os.environ.get('TWITTER_API_KEY')
    api_secret = os.environ.get('TWITTER_API_SECRET_KEY')
    token = os.environ.get('TWITTER_API_ACCESS_TOKEN')
    token_secret = os.environ.get('TWITTER_API_ACCESS_TOKEN_SECRET')
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(token, token_secret)
    return tweepy.API(auth)


def write_twitter_user_request(username):
    api = bootstrap_tweepy()
    user = api.get_user(screen_name=username)
    t = TwitterUser()
    t.username = username
    t.user_id = user.id
    db.session.add(t)
    db.session.commit()


def write_to_hbase_batch_stock(hbase_connection, ticker):
    conn = hbase_connection
    table = conn.table('huarngpa_batch_stock')
    table.put(ticker.encode('utf-8'), {
        b'all:count_trading_days': b'computing',
        b'all:sum_day_open': b'computing',
        b'all:max_day_high': b'computing',
        b'all:min_day_low': b'computing',
        b'all:sum_day_close': b'computing',
        b'all:sum_day_change': b'computing',
        b'all:sum_day_volume': b'computing',
        b'weekly:count_trading_days': b'computing',
        b'weekly:sum_day_open': b'computing',
        b'weekly:max_day_high': b'computing',
        b'weekly:min_day_low': b'computing',
        b'weekly:sum_day_close': b'computing',
        b'weekly:sum_day_change': b'computing',
        b'weekly:sum_day_volume': b'computing',
    })

    
def write_to_hbase_speed_stock(hbase_connection, ticker):
    conn = hbase_connection
    table = conn.table('huarngpa_speed_stock')
    table.put(ticker.encode('utf-8'), {
        b'all:count_trading_days': b'computing',
        b'all:sum_day_open': b'computing',
        b'all:max_day_high': b'computing',
        b'all:min_day_low': b'computing',
        b'all:sum_day_close': b'computing',
        b'all:sum_day_change': b'computing',
        b'all:sum_day_volume': b'computing',
        b'weekly:count_trading_days': b'computing',
        b'weekly:sum_day_open': b'computing',
        b'weekly:max_day_high': b'computing',
        b'weekly:min_day_low': b'computing',
        b'weekly:sum_day_close': b'computing',
        b'weekly:sum_day_change': b'computing',
        b'weekly:sum_day_volume': b'computing',
    })

    
def write_to_hbase_batch_twitter(hbase_connection, user_id):
    conn = hbase_connection
    table = conn.table('huarngpa_batch_twitter')
    table.put(user_id.encode('utf-8'), {
        b'all:min_date': b'computing',
        b'all:max_date': b'computing',
        b'all:count_tweets': b'computing',
        b'all:sum_retweets': b'computing',
        b'all:sum_favorited': b'computing',
        b'weekly:min_date': b'computing',
        b'weekly:max_date': b'computing',
        b'weekly:count_tweets': b'computing',
        b'weekly:sum_retweets': b'computing',
        b'weekly:sum_favorited': b'computing',
    })

    
def write_to_hbase_speed_twitter(hbase_connection, user_id):
    conn = hbase_connection
    table = conn.table('huarngpa_speed_twitter')
    table.put(user_id.encode('utf-8'), {
        b'all:min_date': b'computing',
        b'all:max_date': b'computing',
        b'all:count_tweets': b'computing',
        b'all:sum_retweets': b'computing',
        b'all:sum_favorited': b'computing',
        b'weekly:min_date': b'computing',
        b'weekly:max_date': b'computing',
        b'weekly:count_tweets': b'computing',
        b'weekly:sum_retweets': b'computing',
        b'weekly:sum_favorited': b'computing',
    })
