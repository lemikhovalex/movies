import logging
import time
from functools import wraps

from .utils import process_exception

LOGGER_NAME = "backoff.log"
logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(logging.FileHandler(LOGGER_NAME))


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            done = False
            delay = start_sleep_time
            out = None
            while not done:
                try:
                    out = func(*args, **kwargs)
                    done = True
                except Exception as ex:
                    time.sleep(delay)
                    delay *= factor
                    delay = min(delay, border_sleep_time)
                    process_exception(ex, logger, do_raise=False)
            return out

        return inner

    return func_wrapper
