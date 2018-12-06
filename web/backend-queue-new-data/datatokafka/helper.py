''' Helper classes or functions for the application. '''

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
