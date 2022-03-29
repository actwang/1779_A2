from flask import Flask
from routes.file_routes import file_blueprint
from routes.memcache_routes import memcache_blueprint
from routes.test_routes import test_blueprint
from routes.manager_routes import manager_blueprint

from config import Config
from extensions import mysql

from flask_cors import CORS

frontend_app = Flask(__name__)
CORS(frontend_app)

frontend_app.debug = True

mysql.init_app(frontend_app)

frontend_app.register_blueprint(file_blueprint)
frontend_app.register_blueprint(memcache_blueprint)
frontend_app.register_blueprint(test_blueprint)
frontend_app.register_blueprint(manager_blueprint)

frontend_app.config.from_object(Config.DevConfig)

if __name__ == '__main__':
    frontend_app.run(host='0.0.0.0', port=5000)
