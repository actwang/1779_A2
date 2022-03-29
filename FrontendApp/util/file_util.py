from util.string_processor import get_sha256
import os


def get_file_size(file):
    init_position = file.tell()
    file.seek(0, 2)
    size = file.tell()
    file.seek(init_position)
    return size


def delete_file_from_disk(key, file_name, path):
    target_filename = os.path.join(path, get_sha256(key) + file_name)
    if os.path.exists(target_filename):
        os.remove(target_filename)
