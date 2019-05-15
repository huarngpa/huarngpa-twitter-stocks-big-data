#!/usr/bin/python3

from datetime import datetime
import json
import time

class Tweet:

    def __init__(self, 
                 created_at=None,
                 id_str=None,
                 text=None,
                 retweeted_count=None,
                 favorite_count=None,
                 user_id=None):

        self.created_at = created_at.isoformat()
        self.created_at_day = datetime(created_at.year,
                                       created_at.month,
                                       created_at.day).isoformat()[:10]
        self.id_str = id_str
        self.text = text
        self.retweeted_count = retweeted_count
        self.favorite_count = favorite_count
        self.user_id = user_id

    def __str__(self):
        d = dict()
        d['created_at'] = self.created_at
        d['created_at_day'] = self.created_at_day
        d['id_str'] = self.id_str
        d['text'] = self.text
        d['retweeted_count'] = self.retweeted_count
        d['favorite_count'] = self.favorite_count
        d['user_id'] = self.user_id
        return json.dumps(d)

    def __repr__(self):
        return str(self)


class StockHistory:

    def __init__(self, 
                 ticker=None,
                 date=None,
                 day_open=None,
                 day_high=None,
                 day_low=None,
                 day_close=None,
                 day_volume=None):

        self.ticker = ticker
        self.date = date
        self.day_open = day_open
        self.day_high = day_high
        self.day_low = day_low
        self.day_close = day_close
        self.day_volume = day_volume
        self.received = time.time()

    def __str__(self):
        d = dict()
        d['ticker'] = self.ticker
        d['date'] = self.date
        d['day_open'] = self.day_open
        d['day_high'] = self.day_high
        d['day_low'] = self.day_low
        d['day_close'] = self.day_close
        d['day_volume'] = self.day_volume
        d['received'] = self.received
        return json.dumps(d)

    def __repr__(self):
        return str(self)
