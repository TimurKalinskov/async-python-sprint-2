from exceptions import WorkingTimeoutException


def handler_alarm(signum, frame):
    raise WorkingTimeoutException


