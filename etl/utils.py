def process_exception(exception, logger, do_raise: bool = True):
    logger.exception(str(exception))
    if do_raise:
        raise exception
