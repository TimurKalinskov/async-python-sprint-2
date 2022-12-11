import json

from urllib.request import urlopen
from http import HTTPStatus


def get_api_data():
    url = 'https://code.s3.yandex.net/async-module/moscow-response.json'
    with urlopen(url) as req:
        resp = req.read().decode("utf-8")
        resp = json.loads(resp)
    if req.status != HTTPStatus.OK:
        raise Exception(
            "Error during execute request. {}: {}".format(
                resp.status, resp.reason
            )
        )
    return resp
