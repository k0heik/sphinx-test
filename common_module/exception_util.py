import sys
from logging import Logger


def business_exception(exception_message: str, logger: Logger):
    logger.warning(exception_message)
    sys.exit(1)
