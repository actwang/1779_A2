import hashlib


def get_sha256(input):
    return str(hashlib.sha256(input.encode('utf-8')).hexdigest())
