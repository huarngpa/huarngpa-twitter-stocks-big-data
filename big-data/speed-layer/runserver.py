#!/usr/bin/python3

import argparse
import happybase
import os
import signal
import sys
import time
from .joiner import Joiner


class SpeedLayer:

    def __init__(self):
        self.hbase_host = ''
        self.hbase_port = ''
        self.hbase_connection = None

    def _bootstrap_self(self):
        self.hbase_host = os.environ.get('API_HBASE_HOST')
        self.hbase_port = os.environ.get('API_HBASE_PORT')
        self.hbase_connection = happybase.Connection(self.hbase_host,
                                                     self.hbase_port)
        self._bootstrap_tables()

    def _bootstrap_tables(self):
        conn = self.hbase

    def set_threads(self, num_threads=2):
        pass

    def start(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        self._bootstrap_tables()
        # TODO
        #
        #
        self._terminate()

    def _terminate(self):
        self.end = time.time()
        if self.logging:
            elapsed = self.end - self.start
            print('\nProgram took {0:.2f} seconds.'.format(elapsed))
            print('Processed {} tweets.'.format(self.count))

    def _signal_handler(self, signal, frame):
        print('Bailing out of the program cleanly.')
        self._terminate()
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description='Starts the speed layer.')
    parser.add_argument('-t', '--threads', action='store', dest='threads',
                        help='Speed layer threads to scale with system.')
    args = parser.parse_args()
    helper = SpeedLayer()
    helper.set_threads(args.threads)
    helper.start()


if __name__ == '__main__':
    main()
