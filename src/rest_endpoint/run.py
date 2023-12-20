from flask import Flask

from ynab import ynab_bp
from ynab.rest_endpoint.config import Config


def create_app():
    flask_app = Flask(__name__)
    flask_app.register_blueprint(ynab_bp, url_prefix='/ynab')
    flask_app.config.from_object(Config)
    return flask_app


if __name__ == '__main__':
    app = create_app()
    app.run()
