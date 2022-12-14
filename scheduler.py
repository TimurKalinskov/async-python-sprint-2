import pickle
import os
import logging

from threading import Thread
from queue import Queue
from uuid import uuid4

from job import Job
from exceptions import StopExecution


# logging.basicConfig(level=logging.DEBUG)


class Scheduler:
    tasks_folder = './tasks/'
    statuses_file = 'statuses.txt'
    queue = Queue()
    run_thread = None

    def __init__(self, pool_size=10):
        self.task_coroutine = self._run_task_coroutine()
        self.task_coroutine.send(None)

    def schedule(self, task: Job):
        dependencies = task.dependencies
        task.uid = uuid4()
        with open(self.tasks_folder + str(task.uid), 'wb') as task_file:
            pickle.dump(task, task_file)
            for dt in dependencies:
                dt.uid = uuid4()
                pickle.dump(dt, task_file)
        with open(self.statuses_file, 'a') as status_file: # перезапись файла!!!
            status_file.write(
                f'{task.uid};{task.start_at};{task.tries};'
                f'{task.dependencies};wait\n'
            )

    def start(self):
        self.run_thread = Thread(target=self.run, args=())
        self.run_thread.start()

    def run(self, task_uid=None):
        if task_uid:
            self.task_coroutine.send(task_uid)
            return None
        run_coroutine = self._run_coroutine()
        while True:
            # logging.error('run')
            run_coroutine.send(None)
            if not self.queue.empty():
                try:
                    run_coroutine.throw(StopExecution)
                except StopIteration:
                    return

    def restart(self):
        pass

    def stop(self):
        self.queue.put(StopExecution)
        print('Завершение выполнения задач...')
        self.run_thread.join()

    def _delete_outdated_task(self, task_uid):
        for file in os.listdir(self.tasks_folder):
            if file == task_uid:
                os.remove(self.tasks_folder + file)

    def _refresh_statuses(self, tasks):
        task_ids = [task[0] for task in tasks]
        with open(self.statuses_file, 'r') as file:
            for task_line in file.readlines():
                task = task_line.strip().split(';')
                if task[0] not in task_ids:
                    tasks.append(task)
        with open(self.statuses_file, 'w') as file:
            for task in tasks:
                if task[4] not in ('finished', 'fail'):
                    file.write(';'.join(task) + '\n')
                else:
                    self._delete_outdated_task(task[0])

    def _run_coroutine(self):
        while True:
            try:
                (yield)
            except StopExecution:
                return None
            finally:
                # logging.error('finally')
                actual_tasks = []
                outdated_tasks_indexes = []
                with open(self.statuses_file, 'r') as status_file:
                    for task in status_file.readlines():
                        actual_tasks.append(task.strip().split(';'))
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

    def _run_task_coroutine(self):
        while True:
            task = (yield)
            with open(self.tasks_folder + task, 'rb') as task_file:
                job = pickle.load(task_file)
                yield job.run()
