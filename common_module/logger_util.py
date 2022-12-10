import os
import uuid

from common_module.custom_logger import init_logging, update_record_factory


def get_custom_logger(loggername=None):
    logger = init_logging(loggername)
    log_additions = {
        'request_id': os.environ.get('AWS_BATCH_JOB_ID', uuid.uuid4())
    }
    update_record_factory(**log_additions)

    return logger
