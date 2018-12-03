''' Helper classes or functions for the application. '''

from multiprocessing import Process
from threading import Thread
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


def write_twitter_user_request(username):
    t = TwitterUser()
    t.username = username
    db.session.add(t)
    db.session.commit()
