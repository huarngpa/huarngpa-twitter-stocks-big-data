''' Data classes for the datatokafka application. '''

from datetime import datetime
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class TwitterUser(db.Model):
    __tablename__ = 'twitter_requests'

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255), nullable=False)
    user_id = db.Column(db.String(255), nullable=False)
    requested_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return '<TwitterUser {} - {}>'.format(self.id,
                                              self.username)

    def to_dict(self):
        return dict(id=self.id,
                    username=self.username,
                    user_id=self.user_id,
                    requested_at=self.requested_at)

    @staticmethod
    def in_db(username):
        if TwitterUser.query.filter_by(username=username).first() != None:
            return True
        return False


class StockHistory(db.Model):
    __tablename__ = 'stock_requests'

    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.String(255), nullable=False)
    requested_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return '<StockHistory {} - {}>'.format(self.id,
                                               self.ticker)

    def to_dict(self):
        return dict(id=self.id,
                    ticker=self.ticker,
                    requested_at=self.requested_at)
    
    @staticmethod
    def in_db(ticker):
        if StockHistory.query.filter_by(ticker=ticker).first() != None:
            return True
        return False
