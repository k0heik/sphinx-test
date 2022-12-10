import datetime
import pandas as pd
import numpy as np
import pytest

from module import prepare_df, config


@pytest.fixture
def today():
    return datetime.datetime(2022, 4, 10)


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
                    "campaign_id": [i + 1] * n,
                    "ad_type": ["ad_type"] * n,
                    "ad_id": [i + 10] * n,
                    "date": pd.date_range(end=yesterday, freq="D", periods=n),
                    "clicks": [0] * (n - 1) + [1],
                    "costs": [0] * (n - 1) + [1],
                    "conversions": [0] * (n - 1) + [1],
                    "sales": [0] * (n - 1) + [1],
                }
            )
        )

    return pd.concat(dfs)


@pytest.mark.parametrize("keys", [config.AD_KEY, config.CAMPAIGN_KEY])
@pytest.mark.parametrize("is_denom_zero", [True, False])
@pytest.mark.parametrize("target_kpi, costs_value, expected", [
    ("ROAS", 2, 2),
    ("CPC", 2, 2),
    ("CPA", 2, 2),
])
def test_observed_C_yesterday_in_month(today, datanum, df, keys, is_denom_zero, target_kpi, costs_value, expected):
    if is_denom_zero:
        df["clicks"] = 0
        df["conversions"] = 0
        df["sales"] = 0
        expected = np.inf

    df["costs"] = costs_value

    result_df = prepare_df._observed_C_yesterday_in_month(df, today, target_kpi, keys, "test")

    assert (result_df["test_observed_C_yesterday_in_month"].values == [expected] * datanum).all()


def test_observed_C_yesterday_in_month__first_in_month(df):
    keys = config.AD_KEY
    target_kpi = "ROAS"
    today = datetime.datetime(2022, 4, 1)
    yesterday = today - datetime.timedelta(days=1)
    df["date"] = pd.date_range(end=yesterday, freq="D", periods=len(df))

    result_df = prepare_df._observed_C_yesterday_in_month(df, today, target_kpi, keys, "test")

    assert len(result_df) == 0
