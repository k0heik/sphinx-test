import datetime
import pandas as pd
import numpy as np
import pytest

from common_module.extract_util import complement_daily_ad, fill_0_columns


@pytest.fixture
def df():
    df = pd.DataFrame({
        "advertising_account_id": [1, 1, 1],
        "portfolio_id": [1, 1, 1],
        "ad_group_id": [1, 1, 1],
        "campaign_id": [1, 1, 1],
        "ad_type": ["ad_type", "ad_type", "ad_type"],
        "ad_id": [1, 1, 2],
        "date": pd.to_datetime(["2021-01-27", "2021-01-28", "2021-01-27"])
    })
    df[fill_0_columns] = 100 * np.random.rand(len(df), len(fill_0_columns))
    df["bidding_price"] = 200
    df["uid"] = 1
    return df


@pytest.fixture
def ad_df():
    df = pd.DataFrame({
        "advertising_account_id": [1, 1, 1],
        "portfolio_id": [1, 1, 1],
        "ad_group_id": [1, 1, 1],
        "campaign_id": [1, 1, 1],
        "ad_type": ["ad_type", "ad_type", "ad_type"],
        "ad_id": [1, 2, 3],
    })
    df["match_type"] = "match_type"
    df["account_type"] = "account_type"
    df["campaign_type"] = "campaign_type"
    df["targeting_type"] = "targeting_type"
    df["budget_type"] = "budget_type"
    df["optimization_purpose"] = 1
    df["budget"] = 100000
    df["current_bidding_price"] = 200
    df["uid"] = 1

    return df


def test_complement_daily_ad(df, ad_df):
    complement_date = datetime.datetime(2021, 1, 28)
    result = complement_daily_ad(df, ad_df, complement_date)

    # ad_id 2,3が補完される
    assert len(result) == 5
