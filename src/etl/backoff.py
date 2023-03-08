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
    To call function again in case of an error. It uses naive exponential time delay
    growth until border_sleep_time

    formula:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: initial time
    :param factor: how fast delay time growth
    :param border_sleep_time: upper boundary for delay
    :return: function call result
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
