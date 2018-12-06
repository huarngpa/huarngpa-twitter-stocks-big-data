#!/usr/bin/python3

import argparse
from datetime import datetime
import json
from kafka import KafkaProducer
import os
import signal
import sys
import time
import tweepy
import yaml


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


class TwitterAPI:

    def __init__(self):
        self.api_key = os.environ.get('TWITTER_API_KEY')
        self.api_secret = os.environ.get('TWITTER_API_SECRET_KEY')
        self.token = os.environ.get('TWITTER_API_ACCESS_TOKEN')
        self.token_secret = os.environ.get('TWITTER_API_ACCESS_TOKEN_SECRET')
        self.auth = None
        self.api = None
        self.user = ''
        self.output_fname = ''
        self.kafka_producer = None
        self.kafka_topic = ''
        self.logging = True
        self.start = None
        self.checkpoint = None
        self.end = None
        self.count = 0

    def _bootstrap_api(self):
        self.auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
        self.auth.set_access_token(self.token, self.token_secret)
        self.api = tweepy.API(self.auth)
        kafka_host = os.environ.get('API_KAFKA_HOST')
        self.kafka_producer = KafkaProducer(bootstrap_servers=kafka_host)

    def set_kafka_topic(self, topic=None):
        if topic != None:
            self.kafka_topic = topic

    def _convert_to_object(self, status):
        t = Tweet(status.created_at,
                  status.id_str,
                  status.text,
                  status.retweet_count,
                  status.favorite_count,
                  status.user.id_str)
        return t

    def _append_to_file(self, obj):
        with open(self.output_fname, 'a') as f:
            f.write(str(obj))

    def _write_to_kafka(self, obj):
        self.kafka_producer.send(self.kafka_topic,
                                 str(obj).encode('utf-8'))

    def _iterate_cursor(self):
        self.checkpoint = time.time()
        for status in tweepy.Cursor(self.api.user_timeline, 
                                    screen_name=self.user).items():
            obj = self._convert_to_object(status)
            if self.kafka_topic != '':
                self._write_to_kafka(obj) 
            else:
                self._append_to_file(obj)
            self.count += 1
            if (time.time() - self.checkpoint) > 5:
                print('  Processed {} tweets.'.format(self.count))
                self.checkpoint = time.time()

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
    
    def get(self, user=''):
        signal.signal(signal.SIGINT, self._signal_handler)
        self.count = 0
        self.start = time.time()
        if user != '':
            self.user = user
            self._bootstrap_api()
            self.output_fname = self.user + '.json'
            if self.kafka_topic == '':
                with open(self.output_fname, 'w') as f:
                    pass
            if self.logging:
                print('\nProcessing tweet history for {}.'.format(user))
            self._iterate_cursor()
        self._terminate()


def main():
    parser = argparse.ArgumentParser(description='Get and parse a user\'s tweet history.')
    parser.add_argument('-u', '--user', action='store', dest='twitter_user',
                        help='Twitter user to get.')
    parser.add_argument('-k', '--kafka', action='store', dest='kafka_topic',
                        help='Kafka topic to write data into.')
    args = parser.parse_args()
    helper = TwitterAPI()
    helper.set_kafka_topic(args.kafka_topic)
    helper.get(args.twitter_user)


if __name__ == '__main__':
    main()
