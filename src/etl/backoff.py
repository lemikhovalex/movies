import logging
import os
import time
from functools import wraps

from etl.config import CONFIG
from etl.utils import process_exception

if CONFIG.logger_path is not None:
    LOGGER_NAME = os.path.join(CONFIG.logger_path, "backoff.log")
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(logging.FileHandler(LOGGER_NAME))
else:
    logger = logging


def backoff(
    start_sleep_time=0.1, factor=2, border_sleep_time=10, max_retries: int = 10
):
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
            n_tries = 0
            while (not done) and (n_tries < max_retries):
                try:
                    out = func(*args, **kwargs)
                    done = True
                except Exception as ex:
                    n_tries += 1
                    if n_tries == max_retries:
                        raise ex
                    time.sleep(delay)
                    delay *= factor
                    delay = min(delay, border_sleep_time)
                    process_exception(ex, logger, do_raise=False)
            return out

        return inner

    return func_wrapper
