import pickle
import os

from multiprocessing import Process, Queue, Value
from uuid import uuid4

from job import Job
from exceptions import StopExecution
from utils import scheduler_logger


# logging.basicConfig(level=logging.DEBUG)


class Scheduler:
    tasks_folder = './tasks/'
    statuses_file = 'statuses.txt'
    waiting_tasks_file = 'waiting_tasks.txt'
    queue = Queue()
    run_process = None
    max_tasks = 3
    current_count_tasks = Value('i', 0)

    def __init__(self, pool_size=10):
        self.task_coroutine = self._run_task_coroutine()
        self.task_coroutine.send(None)

    def schedule(self, task: Job):
        if not isinstance(task, Job):
            print('This scheduler supports only Job instances')
            return
        dependencies = task.dependencies
        task.uid = uuid4()
        with open(self.tasks_folder + str(task.uid), 'wb') as task_file:
            pickle.dump(task, task_file)
            for dt in dependencies:
                dt.uid = uuid4()
                pickle.dump(dt, task_file)
        if self.current_count_tasks.value >= self.max_tasks:
            with open(self.waiting_tasks_file, 'a') as waiting_file:
                waiting_file.write(
                    f'{task.uid};{task.start_at};{task.func.__name__};'
                    f'{task.dependencies};wait\n'
                )
                return
        self.current_count_tasks.value += 1
        with open(self.statuses_file, 'a') as status_file:
            status_file.write(
                f'{task.uid};{task.start_at};{task.func.__name__};'
                f'{task.dependencies};wait\n'
            )

    def start(self):
        self.run_process = Process(target=self.run, args=(self.queue,))
        self.run_process.start()

    def run(self, queue, task_uid=None):
        self.queue = queue
        if task_uid:
            status = self.task_coroutine.send(task_uid)
            self._update_single_task_status(task_uid, status)
            return status
        run_coroutine = self._run_coroutine()
        while True:
            # logging.error('run')
            run_coroutine.send(None)
            if not self.queue.empty():
                try:
                    run_coroutine.throw(StopExecution)
                except StopIteration:
                    return

    def stop(self):
        self.queue.put(StopExecution)
        print('Завершение выполнения задач...')
        self.run_process.join()

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
                    waiting_task = self._add_to_queue()
                    if waiting_task.strip():
                        file.write(waiting_task)
                    else:
                        self.current_count_tasks.value -= 1
                    self._delete_outdated_task(task[0])

    def _update_single_task_status(self, task_uid, status=None):
        with open(self.statuses_file, 'r') as status_file:
            all_task_statuses = status_file.readlines()
        if status:
            status = status[1]
        with open(self.statuses_file, 'w') as status_file:
            for task in all_task_statuses:
                if task_uid in task:
                    if status in (1, None):
                        self._delete_outdated_task(task_uid)
                        continue
                status_file.write(task)

    def _add_to_queue(self):
        with open(self.waiting_tasks_file, 'r+') as waiting_file:
            waiting_task = waiting_file.readline()
            data = waiting_file.read()
            waiting_file.seek(0)
            waiting_file.write(data)
            waiting_file.truncate()
            return waiting_task

    def _run_coroutine(self):
        while True:
            try:
                (yield)
            except StopExecution:
                return None
            finally:
                # logging.error('finally')
                actual_tasks = []
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
            if not task:
                yield None
            with open(self.tasks_folder + task, 'rb') as task_file:
                job = pickle.load(task_file)
                yield job.run()
