import signal

from datetime import datetime
from typing import Any

from exceptions import (
    WorkingTimeoutException, RunDateTimeException, TaskErrorException
)
from utils import handler_alarm, task_logger


signal.signal(signal.SIGALRM, handler_alarm)


class Job:
    def __init__(
            self, func, args: list | None = None, kwargs: dict | None = None,
            start_at: str = '', max_working_time: int = 0, tries: int = 1,
            dependencies: list['Job'] | None = None
    ) -> None:
        self.func = func
        self.args = args if args else []
        self.kwargs = kwargs if kwargs else {}
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies if dependencies else []
        self.uid = ''

    def run(self) -> tuple[Any | None, int] | None:
        if not self._check_start_time():
            task_logger.info('Task cannot be run yet')
            return None, 0
        for _ in range(self.tries):
            try:
                self.stop()
                if self.dependencies:
                    self.kwargs = dict(self.kwargs, **self._run_dependencies())
                result = self.func(*self.args, **self.kwargs)
                task_logger.info(
                    f'Task {self.uid}, function {self.func.__name__} finished'
                )
                return result, 1
            except WorkingTimeoutException:
                task_logger.warning(
                    f'{self.func.__name__}: Execution time exceeded'
                )
            except RunDateTimeException:
                task_logger.info(msg='One of the dependencies cannot be run yet')
            except TaskErrorException as er:
                task_logger.error(str(er))
            except Exception as er:
                task_logger.error(
                    f'Task "{self.uid}" function "{self.func.__name__}" '
                    f'raised an exception: {er}'
                )
            finally:
                signal.alarm(0)

    def stop(self) -> None:
        if self.max_working_time > 0:
            signal.alarm(self.max_working_time)

    def _check_start_time(self) -> bool:
        if self.start_at:
            datetime_object = datetime.strptime(
                self.start_at, '%d-%m-%Y %H:%M:%S'
            )
            return datetime.now() > datetime_object
        return True

    def _run_dependencies(self) -> dict:
        results = {}
        for i, job in enumerate(self.dependencies):
            result = job.run()
            if result is None:
                raise TaskErrorException(
                    f'Dependence task {job.uid} {job.func.__name__} '
                    f'raised an exception'
                )
            elif result[1] == 0:
                raise RunDateTimeException
            results[(i, job.func.__name__)] = result[1]
        return results
