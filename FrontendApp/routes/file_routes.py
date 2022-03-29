from flask import Blueprint, request, current_app

from models.error import Error
from models.reply import Reply

from util.string_processor import get_sha256
from util.file_util import get_file_size, delete_file_from_disk

from db.db_access import get_filename_by_key, post_key_filename, get_all_file_keys
from s3 import s3_access

from rpc_calls.memcache_rpcs import call_get, call_put, call_invalidate_key


import base64
import logging
import os

import json

logger = logging.getLogger(__name__)

file_blueprint = Blueprint('file_route', __name__, url_prefix='/api')


@file_blueprint.route('/upload', methods=['POST'])
def post_file():
    key = str(request.form.get('key'))
    file = request.files['file']
    file_size = get_file_size(file)

    if file:
        # Delete existing file with same key
        file_with_same_key = get_filename_by_key(key)
        if file_with_same_key is not None:
            s3_access.delete_file_from_s3(key)
            call_invalidate_key(key)

        if not post_key_filename(key, file.filename, file_size):
            return Reply(success=False, error=Error(500, "Fail to update DB")).to_json()

        s3_access.save_file_to_s3(key, file)

        file.seek(0)
        binary_data = file.read()
        base64_data = base64.b64encode(binary_data)
        base64_msg = base64_data.decode()
        call_put(key, base64_msg)
        return Reply(success=True).to_json()
    else:
        return Reply(success=False).to_json()


@file_blueprint.route('/key/', methods=['POST'])
def get_file():
    key = request.args.get('key')
    if key is None:
        return Reply(success=False, error=Error(400, "No param key is given")).to_json()
    key = str(key)
    # Interact with cache server first.
    response_from_cache = call_get(key)

    if response_from_cache is not None and response_from_cache['content'] != 'None':
        return Reply(success=True, content=response_from_cache['content']).to_json()
    else:
        print("Cache miss. response from cache is : ", response_from_cache)

    file = s3_access.get_file_from_s3(key)
    if file is None:
        return Reply(success=False, error=Error(500, 'No such files available')).to_json()
    binary_data = file.read()
    base64_data = base64.b64encode(binary_data)
    base64_msg = base64_data.decode()
    call_put(key, base64_msg)
    return Reply(success=True, content=base64_msg).to_json()


@file_blueprint.route('/list_keys', methods=['POST'])
def get_all_keys():
    all_keys = get_all_file_keys()
    if all_keys is None:
        return Reply(success=False, error=Error(500, "Failed to execute query")).to_json()
    return Reply(success=True, keys=all_keys).to_json()
