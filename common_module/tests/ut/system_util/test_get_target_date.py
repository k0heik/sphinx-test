import datetime
import pytest
import dateutil

from common_module.system_util import get_target_date


@pytest.mark.parametrize("date", ["latest", "2021-01-01", "2021/01/01"])
def test_get_target_date(date):
    ret = get_target_date(date)

    assert isinstance(ret,  datetime.datetime)


@pytest.mark.parametrize("date", ["latest?", "today", ""])
def test_get_target_date_error(date):
    with pytest.raises(dateutil.parser._parser.ParserError):
        get_target_date(date)
