''' Provides command line utility for interacting with application
    to perform debugging and setup. '''

from flask_script import Manager, Server
from flask_migrate import Migrate, MigrateCommand

from datatokafka.application import create_app
from datatokafka.models import db, TwitterUser, StockHistory


app = create_app()
migrate = Migrate(app, db)
manager = Manager(app)
server = Server(host='0.0.0.0', port=11181)


manager.add_command('db', MigrateCommand)
manager.add_command('runserver', server)


@manager.shell
def shell_ctx():
    return dict(app=app,
                db=db,
                StockHistory=StockHistory,
                TwitterUser=TwitterUser)


if __name__ == '__main__':
    manager.run()
