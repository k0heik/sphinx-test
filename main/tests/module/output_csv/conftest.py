import pandas as pd
import pytest

from module import config


@pytest.fixture
def output_csv_keys_df(ad_info_df):
    return pd.DataFrame(
        {
            "campaign_id": ad_info_df["campaign_id"],
            "ad_type": ad_info_df["ad_type"],
            "ad_id": ad_info_df["ad_id"],
        }
    )


@pytest.fixture
def bid_optimiser_df(output_csv_keys_df):
    n = len(output_csv_keys_df)
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": output_csv_keys_df["campaign_id"],
            "ad_type": output_csv_keys_df["ad_type"],
            "ad_id": output_csv_keys_df["ad_id"],
            "bidding_price": [1] * n,
            "origin_bidding_price": [1] * n,
            "unit_cpc": [0.1] * n,
            "ad_value": [0.2] * n,
            "ad_ema_weekly_cpc": [0.2] * n,
            "is_ml_applied": [True] * n,
            "is_provisional_bidding": [False] * n,
            "has_exception": [False] * n,
            "sum_click_last_four_weeks": [0.1] * n,
            "sum_cost_last_four_weeks": [0.1] * n,
            "cpc_last_four_weeks": [0.1] * n,
        }
    ).astype(config.UNIT_KEY_DTYPES)


@pytest.fixture
def cap_daily_budget_df(output_csv_keys_df):
    keys_df = (
        output_csv_keys_df[config.CAMPAIGN_KEY]
        .groupby(config.CAMPAIGN_KEY)
        .last()
        .reset_index()
    )
    n = len(keys_df)
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": keys_df["campaign_id"],
            "ideal_target_cost": [111] * n,
            "today_target_cost": [222] * n,
            "daily_budget_upper": [100000] * n,
            "total_expected_cost": [300] * n,
            "weight": [0.1] * n,
            "value_of_campaign": [0.11] * n,
            "gradient": [0.2] * n,
            "q": [0.3] * n,
            "max_q": [0.4] * n,
            "has_potential": [True] * n,
            "yesterday_daily_budget": [222] * n,
            "last_week_max_costs": [222] * n,
            "is_daily_budget_undecidable_unit": [False] * n,
            "unit_weekly_cpc_for_cap": [0.1] * n,
        }
    ).astype(config.UNIT_KEY_DTYPES)


@pytest.fixture
def target_pause_df(output_csv_keys_df):
    n = len(output_csv_keys_df)
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": output_csv_keys_df["campaign_id"],
            "ad_type": output_csv_keys_df["ad_type"],
            "ad_id": output_csv_keys_df["ad_id"],
            "is_target_pause": [False] * n,
        }
    ).astype(config.UNIT_KEY_DTYPES)
