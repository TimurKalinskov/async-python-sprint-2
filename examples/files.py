import time


def read_file(path: str) -> list[str]:
    with open(path, 'r') as file:
        return file.readlines()


def update_data(data: list[str]) -> list[str]:
    result = []
    for line in data:
        result.append(line.replace('o', '_').upper())
    return result


def write_to_file(path: str, data: list[str]) -> None:
    with open(path, 'w') as file:
        file.writelines(data)


def long_execution_foo():
    time.sleep(20)
    return
