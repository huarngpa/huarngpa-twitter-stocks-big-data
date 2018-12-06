''' Provides the API endpoints for web client. '''

from flask import Blueprint, jsonify, request, current_app
from datatokafka.get_stock_history import StockAPI
from datatokafka.get_tweets import TwitterAPI
from datatokafka.helper import (start_retrieval_process,
                                write_stock_history_request,
                                write_twitter_user_request)
from datatokafka.models import StockHistory, TwitterUser


api = Blueprint('api', __name__)


@api.route('/hello', methods=('GET',))
def hello():
    data = {'data': 'hello from datatokafka api'}
    return jsonify(data), 200


@api.route('/stock/<ticker>', methods=('GET',))
def stock_api(ticker):
    ticker = ticker.lower()
    data = {'message': ''}
    if not StockHistory.in_db(ticker):
        helper = StockAPI()
        helper.set_kafka_topic('huarngpa_ingest_batch_stock')
        start_retrieval_process(helper, ticker)
        write_stock_history_request(ticker)
        data['message'] = 'Processing 5-year history for {}, this may take some time.'\
            .format(ticker)
        return jsonify(data), 201
    data['message'] = 'Bad request from the user.'
    return jsonify(data), 400


@api.route('/twitter/<user>', methods=('GET',))
def twitter_api(user):
    user = user.lower()
    data = {'message': ''}
    if not TwitterUser.in_db(user):
        helper = TwitterAPI()
        helper.set_kafka_topic('huarngpa_ingest_batch_twitter')
        start_retrieval_process(helper, user)
        write_twitter_user_request(user)
        data['message'] = 'Processing {}\'s tweets, this may take some time.'.format(user)
        return jsonify(data), 201
    data['message'] = 'Bad request from the user.'
    return jsonify(data), 400


@api.route('/stock/requests', methods=('GET',))
def stock_api_requests():
    requests = [s.to_dict() for s in StockHistory.query.all()]
    return jsonify(requests)


@api.route('/twitter/requests', methods=('GET',))
def twitter_api_requests():
    requests = [u.to_dict() for u in TwitterUser.query.all()]
    return jsonify(requests)
