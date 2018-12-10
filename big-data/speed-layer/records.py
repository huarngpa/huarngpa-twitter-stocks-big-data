#!/usr/bin/python3


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
