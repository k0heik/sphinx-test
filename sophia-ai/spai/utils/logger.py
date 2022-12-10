import logging


def get_custom_logger(loggername=None):
    logger = logging.getLogger(loggername)
    logger.setLevel(logging.INFO)

    return logger
