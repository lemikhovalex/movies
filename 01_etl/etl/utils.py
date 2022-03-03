def process_exception(exception, logger):
    logger.exception(str(exception))
    raise exception
