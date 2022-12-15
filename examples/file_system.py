import os


test_directory = 'example_data/'
test_file_name = 'new_file'


def create_directory(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def delete_empty_directory(path: str) -> None:
    if os.path.exists(path):
        os.rmdir(path)


def create_file_in_directory(path: str, name: str) -> None:
    with open(path + name, 'w') as file:
        file.write('test')


def create_directory_with_files(path: str) -> None:
    create_directory(path)
    with open(path + '/1.txt', 'w') as file:
        file.write('test')
    with open(path + '/2.txt', 'w') as file:
        file.write('test2')


def delete_directory_with_files(path: str) -> None:
    for file_name in os.listdir(path):
        file = path + file_name
        if os.path.isfile(file):
            os.remove(file)
    delete_empty_directory(path)


def rename_file(old_name: str, new_name: str) -> None:
    if os.path.exists(old_name):
        os.rename(old_name, new_name)


def error_func():
    os.remove('random_name_for_rise_error')
