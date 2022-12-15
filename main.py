import time

from scheduler import Scheduler
from job import Job
from examples import file_system, files, requests


fs_depend_1 = Job(
    func=file_system.create_directory,
    args=[file_system.test_directory]
)
fs_depend_2 = Job(
    func=file_system.create_file_in_directory,
    args=[file_system.test_directory, file_system.test_file_name]
)
fs_depend_3 = Job(
    func=file_system.rename_file,
    args=[file_system.test_directory + file_system.test_file_name,
          file_system.test_directory + 'new_name']
)
fs_task = Job(
    func=file_system.delete_directory_with_files,
    args=[file_system.test_directory],
    tries=3,
    dependencies=[fs_depend_1, fs_depend_2, fs_depend_3]
)

req_depend_1 = Job(
    func=file_system.create_directory,
    args=[file_system.test_directory]
)
req_depend_2 = Job(
    func=requests.get_data,
    max_working_time=5,
    tries=3,
    return_arg='users'
)
req_depend_3 = Job(
    func=requests.get_user_names,
    return_arg='data'
)
req_task = Job(
    func=files.write_to_file,
    kwargs={
        'path': file_system.test_directory + file_system.test_file_name,
    },
    dependencies=[
        req_depend_1, req_depend_2, req_depend_3
    ]
)

error_task = Job(
    func=file_system.error_func,
    tries=3
)

files_depend_1 = Job(
    func=files.read_file,
    kwargs={
        'path': file_system.test_directory + file_system.test_file_name,
    },
    return_arg='data'
)
files_depend_2 = Job(
    func=files.update_data,
    return_arg='data'
)
files_task = Job(
    func=files.write_to_file,
    args=[file_system.test_directory + 'updated_file.txt'],
    dependencies=[files_depend_1, files_depend_2]
)

infinity_task = Job(
    func=requests.get_data,
    start_at='20-12-2099 15:10:00',
    max_working_time=5,
    tries=3,
    return_arg='users'
)

long_task = Job(
    func=files.long_execution_foo,
    max_working_time=5,
    tries=2,
)

if __name__ == '__main__':
    # Adding tasks
    sh = Scheduler(pool_size=4)
    sh.schedule(fs_task)
    sh.schedule(req_task)
    sh.schedule(files_task)
    sh.schedule(error_task)
    sh.schedule(infinity_task)
    # Stopping scheduler
    time.sleep(1)
    sh.start()
    time.sleep(5)
    sh.stop()
    time.sleep(3)
    sh.schedule(long_task)
    # Restart scheduler
    sh.start()
    time.sleep(2)
    # Adding tasks while working
    sh.schedule(error_task)
    time.sleep(3)
    sh.stop()
