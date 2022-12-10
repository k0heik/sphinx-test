import pytest
import datetime

from spai.optim.models import Performance
from spai.optim.pid import calc_bid_cpc_limit


def data_test_calc_bid_cpc_limit():
    today = datetime.date.today()
    return {
        "basic": (
            {
                "ad_historical_performances": tuple([Performance(*([1] * 9), date=today)] * 14),
                "unit_period_cpc": 1,
            },
            (3, 1)
        ),
        "ad_period_cpc_bigger": (
            {
                "ad_historical_performances": tuple([Performance(*([1] * 4 + [2] * 5), date=today)] * 14),
                "unit_period_cpc": 1,
            },
            (6, 1)
        ),
        "unit_period_cpc_bigger": (
            {
                "ad_historical_performances": tuple([Performance(*([1] * 9), date=today)] * 14),
                "unit_period_cpc": 2,
            },
            (6, 2)
        ),
        "ad_period_cpc_0":
        (
            {
                "ad_historical_performances": tuple([Performance(*([0] * 9), date=today)] * 14),
                "unit_period_cpc": 1,
            },
            (3, 1)
        ),
        "ad_unit_period_cpc_0":
        (
            {
                "ad_historical_performances": tuple([Performance(*([0] * 9), date=today)] * 14),
                "unit_period_cpc": 0,
            },
            (None, None)
        ),
    }


@pytest.mark.parametrize(
    "input, expected",
    data_test_calc_bid_cpc_limit().values(),
    ids=list(data_test_calc_bid_cpc_limit().keys())
)
def test_calc_bid_cpc_limit(input, expected):
    ret = calc_bid_cpc_limit(**input)

    assert ret == expected
