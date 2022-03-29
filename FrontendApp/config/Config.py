import os


class DevConfig(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    UPLOAD_FOLDER = './image_library'
    ALLOWED_FORMAT = {'jpg', 'jpeg', 'png', 'gif', 'tiff'}
    FILENAME_SEPARATOR = '_'
    DB_CONFIG = {
        'endpoint': 'ece1779.cxbccost2b0y.us-east-1.rds.amazonaws.com',
        'port': 3306,
        'region': 'us-east-1',
        'dbname': 'ece1779',
        'user': 'E1779usr',
        'password': '1779Usr!',

        # 'host': 'localhost',
        'database': 'Assignment_1',
        'filename_table': 'file_names',
        'memcache_stat_table': 'cache_stats',
        'memcache_config_table': 'memcache_config'
    }
    # Memcache related macro
    MEMCACHE_SERVER = 'http://192.168.2.104:5001'
    END_POINTS = {
        'put': '/put',
        'get': '/get',
        'clear': '/clear',
        'invalidatekey': '/invalidatekey',
        'refreshconfiguration': '/refreshconfiguration'
    }
