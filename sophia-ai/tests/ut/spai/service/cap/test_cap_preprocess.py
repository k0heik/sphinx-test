import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import pytest

from spai.service.cap.preprocess import CAPPreprocessor, INPUT_COLUMNS, INPUT_DAILY_COLUMNS
from spai.utils.kpi.kpi import MODE_KPI


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "advertising_account_id": [779, 780, 780],
            "portfolio_id": [28, 34, 35],
            "campaign_id": [462118, 462120, 462121],
            "date": [datetime.datetime(2021, 3, 31)] * 3,
            "optimization_costs": [100000, 50000, 10000],
            "cumsum_costs": [20000.1, 20000.1, 20000.1],
            'minimum_daily_budget': [0] * 3,
            'maximum_daily_budget': [10 ** 8] * 3,
            'purpose': ["SALES"] * 3,
            'mode': [MODE_KPI] * 3,
            'budget': [1000] * 3,
            'yesterday_target_cost': [3000] * 3,
            'remaining_days': [1] * 3,
            'ideal_target_cost': [3000] * 3,
            'target_cost': [3000] * 3,
            'noboost_target_cost': [3000] * 3,
            'today_coefficient': [1.0] * 3,
            'yesterday_coefficient': [1.0] * 3,
            'C': [0.1] * 3,
            'unit_ex_observed_C': [0.1] * 3,
            'unit_weekly_ema_costs': [0.1] * 3,
            'campaign_weekly_ema_costs': [0.1] * 3,
            'campaign_observed_C_yesterday_in_month': [0.1] * 3,
            'daily_budget': [1000] * 3,
        }
    )[INPUT_COLUMNS]


@pytest.fixture
def daily_df():
    return pd.DataFrame(
        {
            "advertising_account_id": [779, 780, 780],
            "portfolio_id": [28, 34, 35],
            "campaign_id": [462118, 462120, 462121],
            "date": [datetime.datetime(2021, 3, 30)] * 3,
            "clicks": [4970, 7770, 683],
            "conversions": [2875, 2445, 871],
            "sales": [11393, 15785, 6758],
            "costs": [711.17, 1447.9, 1447.9],
            "cumsum_costs": [20000.1, 20000.1, 20000.1],
            "weight": [np.nan, np.nan, np.nan],
        }
    )[INPUT_DAILY_COLUMNS]


def test_preprocess_zero_size_df(df):
    preprocessor = CAPPreprocessor(df["date"].max())
    result_df, result_daily_df = preprocessor.transform(
        pd.DataFrame(columns=INPUT_COLUMNS), pd.DataFrame(columns=INPUT_COLUMNS))
    assert result_df is not None
    assert len(result_df) == 0
    assert result_daily_df is not None
    assert len(result_daily_df) == 0


def test_preprocess_budget_today_and_yesterday(df, daily_df):
    today = df["date"].max()
    preprocessor = CAPPreprocessor(today)
    result_df, _ = preprocessor.transform(df, daily_df)

    assert list(result_df["today_target_cost"].values) == list(df["target_cost"].values)
    assert list(result_df["yesterday_target_cost"].values) == list(df["yesterday_target_cost"].values)


@pytest.mark.parametrize("is_next_month", [True, False])
def test_comp_df(df, daily_df, is_next_month):
    df["advertising_account_id"] = 1
    df["portfolio_id"] = None
    df["portfolio_id"] = df["portfolio_id"].astype("Int64")
    daily_df["advertising_account_id"] = 1
    daily_df["portfolio_id"] = None
    daily_df["portfolio_id"] = daily_df["portfolio_id"].astype("Int64")
    actual_cols = ["clicks", "conversions", "sales", "costs"]
    for col in actual_cols:
        daily_df[col] = 100

    test_campaign_id = df["campaign_id"].max()
    tmp_daily_df = daily_df[~(daily_df["campaign_id"] == test_campaign_id)].reset_index()
    tmp_daily_df["date"] = tmp_daily_df["date"] + datetime.timedelta(days=1)

    today = df["date"].max()
    if is_next_month:
        today = today + relativedelta(months=1)

    preprocessor = CAPPreprocessor(today)
    result_df = preprocessor._comp_df(df, tmp_daily_df)

    test_df = result_df[(result_df["campaign_id"] == test_campaign_id)]

    assert len(result_df) == len(daily_df)
    if is_next_month:
        assert test_df["used_costs"].values == [0]
    else:
        assert test_df["used_costs"].values == [daily_df["cumsum_costs"].values[0]]
