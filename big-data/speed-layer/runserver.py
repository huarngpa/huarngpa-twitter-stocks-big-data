#!/usr/bin/python3

import argparse
import happybase
from kafka import KafkaProducer
import os
import signal
import sys
import time
import tweepy

from helper import (speed_layer_stock, speed_layer_twitter)
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
        self.hbase_host = os.environ.get('API_HBASE_HOST')
        self.hbase_port = os.environ.get('API_HBASE_PORT')
        self.hbase_connection = happybase.Connection(self.hbase_host,
                                                     int(self.hbase_port))
        self._bootstrap_tables()

    def _bootstrap_tables(self):
        try:
            conn = self.hbase_connection
            conn.create_table('huarngpa_batch_twitter', 
                              {'all': dict(), 'weekly': dict()})
            conn.create_table('huarngpa_batch_stock',
                              {'all': dict(), 'weekly': dict()})
            conn.create_table('huarngpa_batch_twitter_stock',
                              {'linreg': dict()})
            conn.create_table('huarngpa_speed_twitter', 
                              {'all': dict(), 'weekly': dict()})
            conn.create_table('huarngpa_speed_stock',
                              {'all': dict(), 'weekly': dict()})
            conn.create_table('huarngpa_speed_twitter_stock',
                              {'linreg': dict()})
        except Exception as e:
            print(e)

    def set_threads(self, num_threads=2):
        self.num_threads = num_threads
    
    def set_sleep_time(self, seconds=3600):
        self.seconds = seconds

    def start_server(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        self._bootstrap_self()
        while True:
            # TODO
            #
            #
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
