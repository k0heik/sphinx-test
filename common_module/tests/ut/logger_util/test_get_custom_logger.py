import logging

from common_module.logger_util import get_custom_logger


class TestGetCustomLogger:
    def test_delete_partition_success(self):
        logger = get_custom_logger()

        assert type(logger) == logging.Logger
