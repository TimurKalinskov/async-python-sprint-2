import logging
import pickle
import os

from multiprocessing import Process, Queue, Value
from queue import Empty
from uuid import uuid4
from typing import Any, Generator

from job import Job
from exceptions import StopExecution
from utils import scheduler_logger


class Scheduler:
    queue: Queue = Queue()
    run_process = None

    def __init__(self, pool_size=10, tasks_folder: str = './tasks/',
                 statuses_file: str = 'statuses.txt',
                 waiting_tasks_file: str = 'waiting_tasks.txt') -> None:
        self.pool_size = pool_size
        self.tasks_folder = tasks_folder
        self.statuses_file = statuses_file
        self.waiting_tasks_file = waiting_tasks_file
        self.task_coroutine = self._run_task_coroutine()
        self.task_coroutine.send(None)
        self.current_count_tasks = Value('i', 0)
        self.__create_necessary_dependencies()

    def schedule(self, task: Job) -> None:
        if not isinstance(task, Job):
            print('This scheduler supports only Job instances')
            return
        dependencies = task.dependencies
        task.uid = str(uuid4())
        scheduler_logger.info(f'Adding task - {task.uid} {task.func.__name__}')
        with open(self.tasks_folder + str(task.uid), 'wb') as task_file:
            pickle.dump(task, task_file)
            for dt in dependencies:
                dt.uid = str(uuid4())
                pickle.dump(dt, task_file)

        if self.current_count_tasks.value >= self.pool_size:  # type: ignore
            with open(self.waiting_tasks_file, 'a') as waiting_file:
                waiting_file.write(
                    f'{task.uid};{task.start_at};{task.func.__name__};'
                    f'{[d.uid for d in task.dependencies]};wait\n'
                )
                return
        self.current_count_tasks.value += 1  # type: ignore
        with open(self.statuses_file, 'a') as status_file:
            status_file.write(
                f'{task.uid};{task.start_at};{task.func.__name__};'
                f'{[d.uid for d in task.dependencies]};wait\n'
            )

    def start(self) -> None:
        print('Start scheduler')
        self.run_process = Process(target=self.run, args=())
        self.run_process.start()

    def run(self, task_uid: str | None = None) -> tuple[Any | None, int] | None:
        if task_uid:
            scheduler_logger.info(f'Start single task execution "{task_uid}"')
            status = self.task_coroutine.send(task_uid)
            self._update_single_task_status(task_uid, status)
            return status
        scheduler_logger.info(
            f'Start scheduler. Tasks in the queue - '
            f'{self.current_count_tasks.value}'  # type: ignore
        )
        run_coroutine = self._run_coroutine()
        while True:
            run_coroutine.send(None)
            if not self.queue.empty():
                self.__clear_queue()
                try:
                    run_coroutine.throw(StopExecution)
                except StopIteration:
                    return None

    def stop(self) -> None:
        if self.run_process:
            self.queue.put(StopExecution)
            print('Completion of tasks execution...')
            self.run_process.join(10)
            scheduler_logger.info('Stop scheduler execution')
        else:
            print('Scheduler not running')

    def _delete_outdated_task(self, task_uid: str) -> None:
        for file in os.listdir(self.tasks_folder):
            if file == task_uid:
                try:
                    os.remove(self.tasks_folder + file)
                    return
                except OSError as ex:
                    scheduler_logger.error(
                        f'Error while deleting task {task_uid}: {ex}'
                    )
        scheduler_logger.error(f'Cannot find task {task_uid} for deleting')

    def _refresh_statuses(self, tasks: list) -> None:
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
                    waiting_task = self._get_first_in_queue()
                    if waiting_task.strip():
                        file.write(waiting_task)
                    else:
                        self.current_count_tasks.value -= 1  # type: ignore
                    self._delete_outdated_task(task[0])

    def _update_single_task_status(
            self, task_uid: str,
            status: tuple[Any | None, int] | None = None) -> None:
        with open(self.statuses_file, 'r') as status_file:
            all_task_statuses = status_file.readlines()
        if status:
            status = status[1]  # type: ignore
        with open(self.statuses_file, 'w') as status_file:
            for task in all_task_statuses:
                if task_uid in task:
                    if status in (1, None):
                        self._delete_outdated_task(task_uid)
                        continue
                status_file.write(task)

    def _get_first_in_queue(self) -> str:
        with open(self.waiting_tasks_file, 'r+') as waiting_file:
            waiting_task = waiting_file.readline()
            data = waiting_file.read()
            waiting_file.seek(0)
            waiting_file.write(data)
            waiting_file.truncate()
            return waiting_task

    def _run_coroutine(self) -> Generator:
        while True:
            try:
                (yield)
            except StopExecution:
                return None
            finally:
                actual_tasks = []
                with open(self.statuses_file, 'r') as status_file:
                    for task_line in status_file.readlines():
                        actual_tasks.append(task_line.strip().split(';'))
                for task in actual_tasks:
                    status = self.task_coroutine.send(task[0])
                    if not status:
                        task[4] = 'fail'
                        logging.warning(
                            f'Task {task[0]} completed with an error'
                        )
                    elif status[1] == 1:
                        task[4] = 'finished'
                        logging.info(f'Task {task[0]} successfully completed')
                    elif status[1] == 0:
                        task[4] = 'wait'
                    self.task_coroutine.send(None)
                self._refresh_statuses(actual_tasks)

    def _run_task_coroutine(self) -> Generator:
        while task_uid := (yield):
            try:
                with open(self.tasks_folder + task_uid, 'rb') as task_file:
                    job = pickle.load(task_file)
                    yield job.run()
            except FileNotFoundError:
                scheduler_logger.error(f'Task with uid {task_uid} not found')
                yield None

    def __create_necessary_dependencies(self) -> None:
        if not os.path.exists(self.tasks_folder):
            os.makedirs(self.tasks_folder)
        if not os.path.exists(self.waiting_tasks_file):
            with open(self.waiting_tasks_file, 'w'):
                pass
        if not os.path.exists(self.statuses_file):
            with open(self.statuses_file, 'w'):
                pass

    def __clear_queue(self):
        try:
            while True:
                self.queue.get_nowait()
        except Empty:
            pass
