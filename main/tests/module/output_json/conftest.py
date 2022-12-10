import datetime
import pandas as pd
import pytest

from module import config


@pytest.fixture
def output_json_today():
    return datetime.datetime(2021, 3, 12)


@pytest.fixture
def output_json_input_df():
    ad_num = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * ad_num,
            "portfolio_id": [None] * ad_num,
            "campaign_id": range(100, 100 + ad_num),
            "ad_type": ["keyword"] * ad_num,
            "ad_id": range(1, 1 + ad_num),
            "is_paused": [False] * ad_num,
            "daily_budget": [100] * ad_num,
            "bidding_price": [100] * ad_num,
            "last_bidding_price": [999] * ad_num,
            "last_daily_budget": [99999] * ad_num,
            "is_enabled_daily_budget_auto_adjustment": [True] * ad_num,
            "is_enabled_bidding_auto_adjustment": [True] * ad_num,
        }
    )


@pytest.fixture
def bid_optimiser_df(ad_input_json_df):
    ad_keys_df = (
        ad_input_json_df[config.AD_KEY].groupby(config.AD_KEY).last().reset_index()
    )
    n = len(ad_keys_df)
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [ad_input_json_df["campaign_id"].values[0]] * n,
            "ad_type": ad_keys_df["ad_type"],
            "ad_id": ad_keys_df["ad_id"],
            "bidding_price": [1] * n,
        }
    )


@pytest.fixture
def cap_daily_budget_df(ad_input_json_df):
    n = 1
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [ad_input_json_df["campaign_id"].values[0]] * n,
            "daily_budget_upper": [100000] * n,
        }
    )


@pytest.fixture
def target_pause_df(ad_input_json_df):
    ad_keys_df = (
        ad_input_json_df[config.AD_KEY].groupby(config.AD_KEY).last().reset_index()
    )
    n = len(ad_keys_df)
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [ad_input_json_df["campaign_id"].values[0]] * n,
            "ad_type": ad_keys_df["ad_type"],
            "ad_id": ad_keys_df["ad_id"],
            "is_target_pause": [False] * n,
        }
    )
