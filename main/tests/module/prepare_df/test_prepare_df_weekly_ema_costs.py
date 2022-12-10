import datetime
import pandas as pd
import numpy as np
import pytest

from module import prepare_df, config


@pytest.fixture
def today():
    return datetime.datetime(2022, 4, 3)


@pytest.fixture
def datanum():
    return 4


@pytest.fixture
def df(today, datanum):
    yesterday = today - datetime.timedelta(days=1)
    n = 10
    dfs = []
    for i in range(datanum):
        dfs.append(
            pd.DataFrame(
                {
                    "advertising_account_id": [1] * n,
                    "portfolio_id": [None] * n,
                    "campaign_id": 10 if i % 2 == 0 else 20,
                    "ad_type": ["ad_type"] * n,
                    "ad_id": [i + 10] * n,
                    "date": pd.date_range(end=yesterday, freq="D", periods=n),
                    "costs": [0] * (n - 1) + [100],
                }
            )
        )

    return pd.concat(dfs)


@pytest.mark.parametrize("keys, expected_datanum", [
    (config.AD_KEY, 4),
    (config.CAMPAIGN_KEY, 2),
    (config.UNIT_KEY, 1),
])
@pytest.mark.parametrize("is_alldays_same, is_lack", [
    (True, False),
    (False, False),
    (False, True),
])
def test_weekly_ema_costs(today, datanum, df, keys, expected_datanum, is_alldays_same, is_lack):

    if is_lack:
        df = df[df["date"] != today - datetime.timedelta(3)]

    if is_alldays_same:
        df["costs"] = 100
        expected = 100 * (datanum / expected_datanum)
    else:
        expected = 25.307 * (datanum / expected_datanum)

    result_df = prepare_df._weekly_ema_costs(df, today, keys, "test")

    np.testing.assert_array_almost_equal(
        result_df["test_weekly_ema_costs"].values,
        [expected] * expected_datanum,
        decimal=3
    )
