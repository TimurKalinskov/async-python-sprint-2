import time
import unittest
import os

from job import Job
from scheduler import Scheduler
from examples import file_system, files, requests
from utils import is_valid_uuid


class JobTest(unittest.TestCase):
    directory_name = 'test_directory/'

    def tearDown(self) -> None:
        file_system.delete_directory_with_files(self.directory_name)

    def test_simple_task(self) -> None:
        job = Job(
            func=file_system.create_directory, args=[self.directory_name]
        )
        result = job.run()

        self.assertEqual(
            (None, 1),
            result
        )
        self.assertTrue(os.path.exists(self.directory_name))

    def test_too_long_task(self):
        job = Job(
            func=files.long_execution_foo,
            max_working_time=3
        )
        result = job.run()

        self.assertEqual(
            None,
            result
        )

    def test_planned_task(self):
        job = Job(
            func=requests.get_data,
            start_at='20-12-2099 15:10:00'
        )
        result = job.run()

        self.assertEqual(
            (None, 0),
            result
        )

    def test_task_with_tries(self):
        step = 0

        def next_step():
            nonlocal step
            step += 1
            if step < 3:
                raise RuntimeError
            return 'OK'
        job = Job(
            func=next_step,
            tries=3
        )
        result = job.run()

        self.assertEqual(
            ('OK', 1),
            result
        )

    def test_task_with_dependencies(self):
        def get_example_data():
            data = [
                {
                    'name': 'test 1'
                },
                {
                    'name': 'test 2'
                },
                {
                    'name': 'test 3'
                },
            ]
            return data

        dep_job = Job(
            func=get_example_data,
            max_working_time=5,
            tries=3,
            return_arg='users'
        )
        job = Job(
            func=requests.get_user_names,
            return_arg='data',
            dependencies=[dep_job]
        )
        result = job.run()

        self.assertEqual(
            (['test 1\n', 'test 2\n', 'test 3\n'], 1),
            result
        )


class SchedulerTest(unittest.TestCase):
    tasks_folder = './test_tasks/'
    statuses_file = 'test_statuses.txt'
    waiting_tasks_file = 'test_waiting_file.txt'

    def tearDown(self) -> None:
        file_system.delete_directory_with_files(self.tasks_folder)
        if os.path.exists(self.statuses_file):
            os.remove(self.statuses_file)
        if os.path.exists(self.waiting_tasks_file):
            os.remove(self.waiting_tasks_file)

    def test_schedule_task(self):
        sh = Scheduler(
            pool_size=2,
            tasks_folder=self.tasks_folder,
            statuses_file=self.statuses_file,
            waiting_tasks_file=self.waiting_tasks_file
        )
        job1 = Job(requests.get_data)
        sh.schedule(job1)
        job2 = Job(files.long_execution_foo)
        sh.schedule(job2)
        job3 = Job(file_system.error_func)
        sh.schedule(job3)
        uids = []
        for file_name in os.listdir(self.tasks_folder):
            uids.append(file_name)
        self.assertEqual(len(uids), 3)
        self.assertTrue(is_valid_uuid(uids[0]))
        self.assertTrue(is_valid_uuid(uids[1]))

        with open(self.statuses_file, 'r') as status_file:
            statuses = status_file.readlines()
        self.assertEqual(len(statuses), 2)

        with open(self.waiting_tasks_file, 'r') as waiting_file:
            waiting = waiting_file.readlines()
        self.assertEqual(len(waiting), 1)
        self.assertTrue('error_func' in waiting[0])

    def test_run_scheduler(self):
        sh = Scheduler(
            tasks_folder=self.tasks_folder,
            statuses_file=self.statuses_file,
            waiting_tasks_file=self.waiting_tasks_file
        )
        job_error = Job(file_system.error_func)
        sh.schedule(job_error)
        job_plan = Job(
            func=requests.get_data,
            start_at='20-12-2099 15:10:00'
        )
        sh.schedule(job_plan)
        job_get_data = Job(requests.get_data)
        sh.schedule(job_get_data)

        uids = []
        for file_name in os.listdir(self.tasks_folder):
            uids.append(file_name)
        self.assertEqual(len(uids), 3)

        with open(self.statuses_file, 'r') as status_file:
            statuses = status_file.readlines()
        self.assertEqual(len(statuses), 3)
        get_data_task = statuses[0].strip().split(';')
        self.assertTrue(is_valid_uuid(get_data_task[0]))
        self.assertEqual(get_data_task[2], 'error_func')
        self.assertEqual(get_data_task[4], 'wait')

        sh.start()
        time.sleep(3)
        sh.stop()
        uids = []
        for file_name in os.listdir(self.tasks_folder):
            uids.append(file_name)
        self.assertEqual(len(uids), 1)

        with open(self.statuses_file, 'r') as status_file:
            statuses = status_file.readlines()
        self.assertEqual(len(statuses), 1)
        get_data_task = statuses[0].strip().split(';')
        self.assertTrue(is_valid_uuid(get_data_task[0]))
        self.assertEqual(get_data_task[2], 'get_data')
        self.assertEqual(get_data_task[4], 'wait')


if __name__ == "__main__":
    unittest.main()
