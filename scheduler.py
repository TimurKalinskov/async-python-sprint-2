import pickle
import os
import time

from uuid import uuid4

from job import Job


class Scheduler:
    tasks_folder = './tasks/'
    statuses_file = 'statuses.txt'

    def __init__(self, pool_size=10):
        self.task_coroutine = self._run_task()
        self.task_coroutine.send(None)

    def schedule(self, task: Job):
        dependencies = task.dependencies
        task.uid = uuid4()
        with open(self.tasks_folder + str(task.uid), 'wb') as task_file:
            pickle.dump(task, task_file)
            for dt in dependencies:
                dt.uid = uuid4()
                pickle.dump(dt, task_file)
        with open(self.statuses_file, 'a') as status_file:
            status_file.write(
                f'{task.uid};{task.start_at};{task.tries};'
                f'{task.dependencies};wait\n'
            )

    def run(self, task_uid=None):  # Process ?????
        if task_uid:
            self.task_coroutine.send(task_uid)
            return None
        actual_tasks = []
        outdated_tasks_indexes = []
        with open(self.statuses_file, 'r') as status_file:
            for i, task_line in enumerate(status_file.readlines()):
                task = task_line.strip().split(';')
                if task[4] in ('finished', 'fail'):
                    outdated_tasks_indexes.append(i)
                    self._delete_outdated_task(task[0])
                else:
                    actual_tasks.append(task)
            self._clean_statuses(outdated_tasks_indexes)
        for task in actual_tasks:
            status = self.task_coroutine.send(task[0])
            if not status:
                task[4] = 'fail'
            elif status[1] == 1:
                task[4] = 'finished'
            elif status[1] == 0:
                task[4] = 'wait'
            self.task_coroutine.send(None)
        self._refresh_statuses(actual_tasks)

    def restart(self):
        pass

    def stop(self):
        pass

    def _delete_outdated_task(self, task_uid):
        for file in os.listdir(self.tasks_folder):
            if file == task_uid:
                os.remove(self.tasks_folder + file)

    def _clean_statuses(self, indexes):
        with open(self.statuses_file, 'r') as file:
            tasks = file.readlines()
        with open(self.statuses_file, 'w') as file:
            for i, line in enumerate(tasks):
                if i not in indexes:
                    file.write(line)

    def _refresh_statuses(self, tasks):
        with open(self.statuses_file, 'w') as file:
            for task in tasks:
                file.write(';'.join(task) + '\n')

    def _run_task(self):
        while True:
            task = (yield)
            with open(self.tasks_folder + task, 'rb') as task_file:
                job = pickle.load(task_file)
                yield job.run()
