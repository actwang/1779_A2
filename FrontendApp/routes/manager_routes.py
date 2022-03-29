from flask import Blueprint, request, current_app

from models.reply import Reply

from db import db_access
from s3 import s3_access
from rpc_calls import memcache_rpcs

manager_blueprint = Blueprint('manager_route', __name__, url_prefix='/api/manager')


@manager_blueprint.route('/clear', methods=['POST'])
def post_clear_files():
    all_file_keys = db_access.get_all_file_keys()
    for file_key in all_file_keys:
        try:
            s3_access.delete_file_from_s3(file_key)
        except Exception as e:
            print(f'Failed to delete key {file_key}')
            print(e)
            return Reply(success=False).to_json()
    db_access.delete_all_files()

    # Clear all caches from all cache instances.
    memcache_rpcs.call_clear()
    return Reply(success=True).to_json()
