def post_table_query():
    return 'create table file_names (  file_key  VARCHAR(32) not null, file_name VARCHAR(32) null,    constraint ' \
           'file_names_pk        primary key (file_key)); '


def get_filename_query(table, key):
    return f'SELECT file_name FROM {table} WHERE  file_key=\'{key}\';'


def get_caches_url_query():
    return 'SELECT private_url FROM cache_instances WHERE is_avail=true'


def get_all_file_key_query(table):
    return f'SELECT file_key FROM {table};'


def post_file_key_and_name(table, file_key, file_name, file_size):
    return f"INSERT INTO {table} VALUE (\'{file_key}\', \'{file_name}\', \'{file_size}\')" + \
           'ON DUPLICATE KEY UPDATE file_name=\'{name}\', '.format(name=file_name) + \
           f' file_size=\'{file_size}\';'


def delete_cache_stat(table):
    return "DELETE FROM {table} WHERE capacity>= 0;".format(table=table)


def delete_all_files_query(table):
    return f'DELETE FROM {table}'


def get_cache_stat(table, date='2022-02-18 09:05:00'):
    return "SELECT * FROM {table} WHERE time_stamp>='{d}';".format(table=table, d=date)


def post_update_cache_config(table, capacity, value):
    return f"INSERT INTO {table} VALUE (\'{capacity}\', \'{value}\')" + \
           'ON DUPLICATE KEY UPDATE rep_policy=\'{value}\'; '.format(value=value)
