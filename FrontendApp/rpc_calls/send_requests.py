import requests
import json

TIMEOUT_IN_SECONDS = 1
TIMEOUT_RESPONSE = json.dumps({'content': 'None'})


def post(url, data=None):
    try:
        res = requests.post(url, data=data, timeout=TIMEOUT_IN_SECONDS)
        print(res)
        return res
    except TimeoutError as e:
        print(f'{url}: Timeout')
        print(e)
        return None
    except Exception as e:
        print(e)
        return None
