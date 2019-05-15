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


def get_serving_layer_data(hbase_connection, topic):
    conn = hbase_connection
    table = conn.table(topic)
    result = dict()
    for key, data in table.scan():
        dkey = key.decode(encoding='UTF-8')
        result[dkey] = dict()
        for k, v in data.items():
            try:
                dk = k.decode(encoding='UTF-8')
                dv = v.decode(encoding='UTF-8')
                result[dkey][dk] = dv
            except Exception as e:
                # fails silently
                # print(e, k, v)
                pass
    return result


def merge_serving_layer_data(left, right):
    lkeys = left.keys()
    rkeys = right.keys()
    mset = set(list(lkeys) + list(rkeys))
    merged = dict()
    for key in mset:
        merged[key] = dict()
        left_data = left[key] if left.get(key) != None else dict()
        right_data = right[key] if right.get(key) != None else dict()
        iter_dict = left_data if len(left_data.keys()) > 0 else right_data
        for k, v in iter_dict.items():
            if k[-4:] == 'date':
                continue
            val = 0
            try:
                val += float(left_data[k]) if left_data.get(k) != None else 0
            except ValueError:
                pass
            try:
                val += float(right_data[k]) if right_data.get(k) != None else 0
            except ValueError:
                pass
            merged[key][k] = val
    return merged


def ensure_key_exists(d, k):
    d[k] = "N/A" if d.get(k) == None else d[k]


def try_divide_by(r, k, n, d):
    try:
        r[k] = round(float(r[n]) / float(r[d]), 3)
    except Exception as e:
        pass


def format_two_precision(d, k):
    try:
        d[k] = "{0:.2f}".format(d[k])
    except Exception as e:
        pass


def format_three_precision(d, k):
    try:
        d[k] = "{0:.3f}".format(d[k])
    except Exception as e:
        pass


def format_with_commas(d, k):
    try:
        d[k] = "{0:,}".format(int(d[k]))
    except Exception as e:
        pass


def patch_and_format_twitter_data(merged):
    data = []
    api = bootstrap_tweepy()
    for k, v in merged.items():
        user = api.get_user(id=k)
        v['user_id'] = k
        v['username'] = user.screen_name
        ensure_key_exists(v, 'weekly:count_tweets')
        ensure_key_exists(v, 'weekly:sum_favorited')
        ensure_key_exists(v, 'weekly:sum_retweets')
        ensure_key_exists(v, 'weekly:sum_sentiment')
        ensure_key_exists(v, 'weekly:sentiment')
        try_divide_by(v, 'weekly:sentiment', 
                      'weekly:sum_sentiment', 'weekly:count_tweets')
        format_with_commas(v, 'weekly:count_tweets')
        format_with_commas(v, 'weekly:sum_favorited')
        format_with_commas(v, 'weekly:sum_retweets')
        format_with_commas(v, 'weekly:sum_sentiment')
        format_three_precision(v, 'weekly:sentiment')
        data.append(v)
    return data


def patch_and_format_stock_data(merged):
    data = []
    for k, v in merged.items():
        v['ticker'] = k
        ensure_key_exists(v, 'weekly:count_trading_days')
        ensure_key_exists(v, 'weekly:max_day_high')
        ensure_key_exists(v, 'weekly:min_day_low')
        ensure_key_exists(v, 'weekly:sum_day_change')
        ensure_key_exists(v, 'weekly:sum_day_close')
        ensure_key_exists(v, 'weekly:sum_day_open')
        ensure_key_exists(v, 'weekly:sum_day_volume')
        ensure_key_exists(v, 'weekly:avg_day_change')
        ensure_key_exists(v, 'weekly:avg_day_open')
        ensure_key_exists(v, 'weekly:avg_day_close')
        ensure_key_exists(v, 'weekly:avg_day_volume')
        try_divide_by(v, 'weekly:avg_day_change', 
                      'weekly:sum_day_change', 
                      'weekly:count_trading_days')
        try_divide_by(v, 'weekly:avg_day_open', 
                      'weekly:sum_day_open', 
                      'weekly:count_trading_days')
        try_divide_by(v, 'weekly:avg_day_close', 
                      'weekly:sum_day_close', 
                      'weekly:count_trading_days')
        try_divide_by(v, 'weekly:avg_day_volume', 
                      'weekly:sum_day_volume', 
                      'weekly:count_trading_days')
        format_three_precision(v, 'weekly:avg_day_change')
        format_two_precision(v, 'weekly:avg_day_open')
        format_two_precision(v, 'weekly:avg_day_close')
        format_two_precision(v, 'weekly:max_day_high')
        format_two_precision(v, 'weekly:min_day_low')
        format_with_commas(v, 'weekly:avg_day_volume')
        data.append(v)
    return data
