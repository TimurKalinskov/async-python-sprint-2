import logging
import uuid

from exceptions import WorkingTimeoutException


def handler_alarm(signum, frame):
    raise WorkingTimeoutException


def config_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


task_logger = config_logger('task_logger', 'tasks.log')
scheduler_logger = config_logger('scheduler_logger', 'scheduler.log')


def is_valid_uuid(value):
    try:
        uuid.UUID(str(value))
        return True
    except ValueError:
        return False
