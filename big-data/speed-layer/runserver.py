#!/usr/bin/python3

import argparse
import happybase
from kafka import KafkaProducer
import os
import signal
import sys
import time
import tweepy

from helper import *
from joiner import Joiner


class SpeedLayer:

    def __init__(self):
        self.hbase_host = ''
        self.hbase_port = ''
        self.hbase_connection = None
        self.num_threads = None 
        self.seconds = None
        self.logging = True
        self.start = None
        self.end = None

    def _bootstrap_self(self):
        self.start = time.time()
        self.hbase_host = os.environ.get('API_HBASE_THRIFT_HOST')
        self.hbase_port = os.environ.get('API_HBASE_THRIFT_PORT')
        self.hbase_connection = happybase.Connection(self.hbase_host,
                                                     int(self.hbase_port))
        self._bootstrap_tables()

    def _bootstrap_tables(self):
        try:
            create_hbase_tables(self.hbase_connection)
        except Exception as e:
            print(e)

    def set_threads(self, num_threads=2):
        self.num_threads = num_threads
    
    def set_sleep_time(self, seconds=3600):
        self.seconds = seconds

    def _get_speed_layer_twitter(self):
        keys = get_twitter_keys(self.hbase_connection)
        for key in keys:
            dkey = key.decode('utf-8')
            data = speed_layer_twitter_data(dkey)
            objects = convert_statuses_to_objects(data)
            write_to_batch_layer_twitter(objects)
            write_to_serving_layer_twitter(self.hbase_connection,
                                           dkey,
                                           objects)
            print('{}: Speed layer handled twitter user: {}.'.format(time.time(),
                                                                     dkey))
    
    def _get_speed_layer_stock(self):
        keys = get_stock_keys(self.hbase_connection)
        for key in keys:
            dkey = key.decode('utf-8')
            data = speed_layer_stock_data(dkey)
            obj = convert_to_stock_object(data)
            write_to_batch_layer_stock(obj)
            write_to_serving_layer_stock(self.hbase_connection, obj)
            print('{}: Speed layer handled stock ticker: {}.'.format(time.time(),
                                                                     dkey))

    def start_server(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        self._bootstrap_self()
        while True:
            self._get_speed_layer_twitter()
            self._get_speed_layer_stock()
            time.sleep(self.seconds)
        self._terminate()

    def _terminate(self):
        self.end = time.time()
        if self.logging:
            elapsed = self.end - self.start

    def _signal_handler(self, signal, frame):
        print('Bailing out of the program cleanly.')
        self._terminate()
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description='Starts the speed layer.')
    parser.add_argument('-t', '--threads', action='store', dest='threads',
                        help='Speed layer threads to scale with system.')
    parser.add_argument('-s', '--seconds', action='store', dest='seconds',
                        help='Sleep settings for the speed layer.')
    args = parser.parse_args()
    helper = SpeedLayer()
    helper.set_threads(args.threads)
    helper.set_sleep_time(int(args.seconds))
    helper.start_server()


if __name__ == '__main__':
    main()
