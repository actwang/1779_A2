import datetime

#import extensions
import pymysql
from config.Config import DevConfig

from db.query import get_filename_query, get_all_file_key_query, post_file_key_and_name, get_cache_stat, \
    post_update_cache_config, delete_cache_stat, get_caches_url_query, delete_all_files_query


def get_connection():
    config = DevConfig.DB_CONFIG
    try:
        conn = pymysql.connect(
                         host=config['endpoint'],
                         port=config['port'],
                         user=config['user'],
                         password=config['password'],
                         database='ECE1779')
    except Exception as e:
        print(e)
        return None
    return conn


def get_filename_by_key(key):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        query = get_filename_query(DevConfig.DB_CONFIG['filename_table'], key)
        cursor.execute(query)
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return row[0]
    except Exception:
        return None


def get_all_file_keys():
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        query = get_all_file_key_query(DevConfig.DB_CONFIG['filename_table'])
        cursor.execute(query)
        nested_file_keys = cursor.fetchall()
        file_keys = [nested_file_keys[i][0] for i in range(len(nested_file_keys))]
        # In the form of [[k1], [k2]...]
        if file_keys is None:
            return None
        else:
            return file_keys
    except Exception as e:
        print(e)
        return []


def post_key_filename(file_key, file_name, file_size):
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        query = post_file_key_and_name(DevConfig.DB_CONFIG['filename_table'], file_key, file_name, file_size)
        rows_affect = cursor.execute(query)
        cnx.commit()
        return True
    except Exception as err:
        print(err)
        return False


def get_all_avail_cache_instances_url():
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        cursor.execute(get_caches_url_query())
        cnx.commit()
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(e)
        return False


def init_tables():
    try:
        cnx = get_connection()
        return True
    except Exception:
        return False


def delete_all_files():
    try:
        cnx = get_connection()
        cursor = cnx.cursor()
        delete_query = delete_all_files_query(DevConfig.DB_CONFIG['filename_table'])
        cursor.execute(delete_query)
        cnx.commit()
        return True
    except Exception as e:
        print(e)
        return False
