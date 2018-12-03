''' Creates a Flask app instance and registers the database object '''


from flask import Flask


def create_app(app_name='DATA_TO_KAFKA_API'):

    app = Flask(app_name)
    app.config.from_object('datatokafka.config.BaseConfig')

    from datatokafka.api import api
    app.register_blueprint(api, url_prefix='/api')

    from datatokafka.models import db
    db.init_app(app)

    return app
