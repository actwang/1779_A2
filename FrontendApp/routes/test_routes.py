from flask import Blueprint, request, current_app
from util.file_util import get_file_size, delete_file_from_disk

from models.error import Error
from models.reply import Reply

from s3 import s3_access

from db import db_access

import base64


test_blueprint = Blueprint('test_route', __name__, url_prefix='/api/test')


@test_blueprint.route('/upload', methods=['POST'])
def post_file():
    key = str(request.form.get('key'))
    file = request.files['file']
    file_size = get_file_size(file)

    if file:
        s3_access.save_file_to_s3(key, file,)
        return Reply(success=True).to_json()
    else:
        return Reply(success=False).to_json()


@test_blueprint.route('/key/', methods=['POST'])
def get_file():
    key = request.args.get('key')
    if key is None:
        return Reply(success=False, error=Error(400, "No param key is given")).to_json()
    key = str(key)
    file = s3_access.get_file_from_s3(key)
    binary_data = file.read()
    base64_data = base64.b64encode(binary_data)
    base64_msg = base64_data.decode()
    return Reply(success=True, content=base64_msg).to_json()


@test_blueprint.route('/sql', methods=['POST'])
def sql():
    db_access.init_tables()
    return Reply(success=True, content='who').to_json()
