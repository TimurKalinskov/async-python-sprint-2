import json

from urllib.request import urlopen, Request
from http import HTTPStatus


def get_data() -> list[dict]:
    url = 'https://jsonplaceholder.typicode.com/users'
    with urlopen(url) as response:
        if response.status != HTTPStatus.OK:
            raise Exception(
                "Error during execute request. {}: {}".format(
                    response.status, response.reason
                )
            )
        data = response.read().decode("utf-8")
        data = json.loads(data)
        return data


def get_user_names(users: list[dict]) -> list[str]:
    user_names = [user['name'] + '\n' for user in users]
    return user_names


def example_str_foo() -> str:
    return 'Test result data'
