import logging
import pytest

from common_module.exception_util import business_exception


class TestBusinessException:
    @classmethod
    def setup_class(cls):
        cls._logger = logging.getLogger()

    def test_call(self):
        with pytest.raises(SystemExit) as e:
            business_exception('test', self._logger)

            assert e.value.code == 1
