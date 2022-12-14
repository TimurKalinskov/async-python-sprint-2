from datetime import datetime
import signal

from exceptions import WorkingTimeoutException, RunDateTimeException
from utils import handler_alarm


signal.signal(signal.SIGALRM, handler_alarm)


class Job:
    def __init__(
            self, func, args: list = [], kwargs: dict = {}, start_at='',
            max_working_time=-1, tries=1, dependencies=[]
    ):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.uid = ''

    def run(self):
        if not self._check_start_time():
            return None, 0
        for _ in range(self.tries):
            try:
                self.stop()
                if self.dependencies:
                    self.kwargs = dict(self.kwargs, **self._run_dependencies())
                result = self.func(*self.args, **self.kwargs)
                return result, 1
            except WorkingTimeoutException:
                print(f'{self.func.__name__}: Execution time exceeded')
            except RunDateTimeException:
                print(f'One of the dependencies cannot be run yet')
                signal.alarm(0)

    def stop(self):
        if self.max_working_time > 0:
            signal.alarm(self.max_working_time)

    def _check_start_time(self):
        if self.start_at:
            datetime_object = datetime.strptime(
                self.start_at, '%d-%m-%Y %H:%M:%S'
            )
            return datetime.now() > datetime_object
        return True

    def _run_dependencies(self):
        results = {}
        for job in self.dependencies:
            result = job.run()
            if result is None:
                raise WorkingTimeoutException
            elif result[1] == 0:
                raise RunDateTimeException
            results[job.func.__name__] = result[1]
        return results
