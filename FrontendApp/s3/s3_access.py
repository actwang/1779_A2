from extensions import s3


def save_file_to_s3(key, file, bucket_name='1779test'):
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=file)
    except Exception as e:
        print(e)
        return False


def get_file_from_s3(key, bucket_name='1779test'):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        print(e)
        return None
    return response['Body']


def delete_file_from_s3(key, bucket_name='1779test'):
    try:
        response = s3.delete_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        print(e)
        return False
    return response
