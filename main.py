import time

from scheduler import Scheduler
from job import Job


sh = Scheduler()


def s(a, b):
    time.sleep(3)
    return a + b


def d(x, y):
    return x - y


def error_func():
    return 1 / 0



# j1 = Job(s, '11-12-2022 15:10:00')
j2 = Job(d, [7, 6], start_at='20-12-2022 15:10:00')
j3 = Job(s, [5, 3], max_working_time=3, tries=3, dependencies=[j2],
         start_at='20-12-2022 15:10:00')
j4 = Job(d, [10, 3], start_at='12-10-2022 15:10:00')
j5 = Job(s, [10, 10])
j6 = Job(s, [5, 3], max_working_time=2, tries=3, dependencies=[j2])
j7 = Job(s, [5, 3], max_working_time=2, tries=3)
j8 = Job(error_func, max_working_time=2, tries=3)
j9 = Job(s, [5, 3], max_working_time=5, tries=3, dependencies=[j8])

# sh.schedule(j2)
# sh.schedule(j3)
# sh.schedule(j4)
# sh.schedule(j5)
# sh.start()
# print('bla bla')
# time.sleep(2)
# sh.stop()
