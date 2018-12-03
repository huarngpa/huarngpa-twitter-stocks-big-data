#!/usr/bin/python3

import argparse
from datetime import datetime
from iexfinance.stocks import get_historical_data
import json
from kafka import KafkaProducer
import os
import signal
import sys
import time


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

    def __str__(self):
        d = dict()
        d['ticker'] = self.ticker
        d['date'] = self.date
        d['day_open'] = self.day_open
        d['day_high'] = self.day_high
        d['day_low'] = self.day_low
        d['day_close'] = self.day_close
        d['day_volume'] = self.day_volume
        return json.dumps(d)

    def __repr__(self):
        return str(self)


class StockAPI:
    
    def __init__(self):
        self.ticker = ''
        self.output_fname = ''
        self.kafka_producer = None
        self.kafka_topic = ''
        self.logging = True
        self.start = None
        self.checkpoint = None
        self.end = None
        self.count = 0
    
    def _bootstrap_api(self):
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    def set_kafka_topic(self, topic=None):
        if topic != None:
            self.kafka_topic = topic
    
    def _convert_to_object(self, day, daily):
        h = StockHistory(self.ticker,
                         day,
                         daily['open'],
                         daily['high'],
                         daily['low'],
                         daily['close'],
                         daily['volume'])
        return h
    
    def _append_to_file(self, obj):
        with open(self.output_fname, 'a') as f:
            f.write(str(obj))
    
    def _write_to_kafka(self, obj):
        self.kafka_producer.send(self.kafka_topic,
                                 str(obj).encode('utf-8'))

    def _iterate_cursor(self):
        now = datetime.now()
        then = datetime(now.year - 5, now.month, now.day)
        today = datetime(now.year, now.month, now.day)
        self.checkpoint = time.time()
        data = get_historical_data(self.ticker,
                                   then,
                                   today,
                                   output_format='json')
        for day, daily in data.items():
            obj = self._convert_to_object(day, daily)
            if self.kafka_topic != '':
                self._write_to_kafka(obj) 
            else:
                self._append_to_file(obj)
            self.count += 1
            if (time.time() - self.checkpoint) > 5:
                print('  Processed {} trading days.'.format(self.count))
                self.checkpoint = time.time()

    def _terminate(self):
        self.end = time.time()
        if self.logging:
            elapsed = self.end - self.start
            print('\nProgram took {0:.2f} seconds.'.format(elapsed))
            print('Processed {} trading days.'.format(self.count))

    def _signal_handler(self, signal, frame):
        print('Bailing out of the program cleanly.')
        self._terminate()
        sys.exit(0)
    
    def get(self, ticker=''):
        signal.signal(signal.SIGINT, self._signal_handler)
        self.count = 0
        self.start = time.time()
        if ticker != '':
            self.ticker = ticker
            self._bootstrap_api()
            self.output_fname = self.ticker + '.json'
            if self.kafka_topic == '':
                with open(self.output_fname, 'w') as f:
                    pass
            if self.logging:
                print('\nProcessing trading history for {}.'.format(ticker))
            self._iterate_cursor()
        self._terminate()


def main():
    parser = argparse.ArgumentParser(description='Get up to five year\'s worth of stock history.')
    parser.add_argument('-t', '--ticker', action='store', dest='ticker',
                        help='Ticker to retrieve.')
    parser.add_argument('-k', '--kafka', action='store', dest='kafka_topic',
                        help='Kafka topic to write data into.')
    args = parser.parse_args()
    helper = StockAPI()
    helper.set_kafka_topic(args.kafka_topic)
    helper.get(args.ticker)


if __name__ == '__main__':
    main()
