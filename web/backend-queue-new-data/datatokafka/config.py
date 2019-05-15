''' Settings for the Flask application. '''


class BaseConfig(object):
    DEBUG = True
    SECRET_KEY = 'EJkAZ9d7mkNub1mqE0su'
    SQLALCHEMY_DATABASE_URI = 'sqlite:///twitterstocksdb'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
