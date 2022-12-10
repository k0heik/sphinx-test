import datetime
import pandas as pd
import pytest


from module.cap_daily_budget import _INPUT_DF_COLS, _INPUT_DAILY_DF_COLS


@pytest.fixture
def cap_daily_budget_today(ad_target_actual_df):
    return ad_target_actual_df["date"].max() + datetime.timedelta(days=1)


@pytest.fixture
def cap_daily_budget_input_df(cap_daily_budget_today):
    return pd.DataFrame(
        {
            "advertising_account_id": [779],
            "portfolio_id": [None],
            "campaign_id": [462118],
            "date": [cap_daily_budget_today],
            "optimization_costs": [100000],
            "minimum_daily_budget": [0],
            "maximum_daily_budget": [10**8],
            "purpose": ["SALES"],
            "daily_budget": [1000],
            "yesterday_target_cost": [3000],
            "remaining_days": [10],
            "ideal_target_cost": [3000],
            "target_cost": [3000],
            "noboost_target_cost": [3000],
            "today_coefficient": [1.0],
            "yesterday_coefficient": [1.0],
            "C": [0.1],
            "mode": ["BUDGET"],
            "unit_ex_observed_C": [1.0],
            "unit_weekly_ema_costs": [100000],
            "campaign_weekly_ema_costs": [0],
            "campaign_observed_C_yesterday_in_month": [1.0],
        }
    )[_INPUT_DF_COLS]


@pytest.fixture
def cap_daily_budget_input_daily_df(cap_daily_budget_today):
    yesterday = cap_daily_budget_today - datetime.timedelta(days=1)
    days = 31
    return pd.DataFrame(
        {
            "advertising_account_id": [779] * days,
            "portfolio_id": [None] * days,
            "campaign_id": [462118] * days,
            "date": pd.date_range(end=yesterday, freq="D", periods=days),
            "clicks": [4970] * days,
            "conversions": [2875] * days,
            "sales": [11393] * days,
            "costs": [711.17] * days,
            "optimization_costs": [100000] * days,
            "minimum_daily_budget": [0] * days,
            "maximum_daily_budget": [10**8] * days,
            "cumsum_costs": [20000.1] * days,
            "weight": [None] * days,
            "purpose": ["SALES"] * days,
        }
    )[_INPUT_DAILY_DF_COLS]
