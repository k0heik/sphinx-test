import datetime
import numpy as np
import pandas as pd
import pytest

from spai.service.cap.preprocess import CAPPreprocessor
from spai.service.cap.calculator import CAPCalculator
from spai.service.cap.service import CAPCalculationService
from spai.utils.kpi.kpi import MODE_KPI


@pytest.fixture
def today():
    return datetime.datetime(2021, 4, 27)


@pytest.fixture
def df(today):
    return pd.DataFrame(
        {
            "advertising_account_id": [779, 780, 780],
            "portfolio_id": [28, 34, 34],
            "date": [today] * 3,
            "campaign_id": [462118, 462120, 462121],
            "minimum_daily_budget": [0] * 3,
            "maximum_daily_budget": [10 ** 8] * 3,
            "purpose": ["SALES"] * 3,
            'mode': [MODE_KPI] * 3,
            "budget": [1000] * 3,
            "yesterday_target_cost": [3000] * 3,
            "remaining_days": [4] * 3,
            "ideal_target_cost": [3000] * 3,
            "target_cost": [3000] * 3,
            "noboost_target_cost": [3000] * 3,
            "today_coefficient": [1.0] * 3,
            "yesterday_coefficient": [1.0] * 3,
            'C': [0.1] * 3,
            'unit_ex_observed_C': [0.1] * 3,
            'unit_weekly_ema_costs': [0.1] * 3,
            'campaign_weekly_ema_costs': [0.1] * 3,
            'campaign_observed_C_yesterday_in_month': [0.1] * 3,
        }
    )


@pytest.fixture
def daily_df(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df = pd.DataFrame(
            {
                "advertising_account_id": [779, 780, 780],
                "portfolio_id": [28, 34, 34],
                "campaign_id": [462118, 462120, 462121],
                "clicks": [4970, 7770, 683],
                "conversions": [2875, 2445, 871],
                "sales": [11393, 15785, 6758],
                "costs": [711.17, 1447.9, 1447.9],
                "optimization_costs": [100000, 50000, 10000],
                "daily_budget": [1000] * 3,
                "cumsum_costs": [20000.1, 20000.1, 20000.1],
                "weight": [np.nan, np.nan, np.nan],
            }
        )
        df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(df)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    return df


@pytest.fixture
def campaign_all_actual_df(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df = pd.DataFrame(
            {
                "advertising_account_id": [779, 780, 780],
                "portfolio_id": [28, 34, 34],
                "campaign_id": [462118, 462120, 462121],
                "clicks": [4970, 7770, 683],
                "conversions": [2875, 2445, 871],
                "sales": [11393, 15785, 6758],
                "costs": [711.17, 1447.9, 1447.9],
                "purpose": ["SALES"] * 3,
            }
        )
        df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(df)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    return df


def test_transform(today, df, daily_df, campaign_all_actual_df):
    preprocessor = CAPPreprocessor(today)
    calculator = CAPCalculator(today)
    service = CAPCalculationService(preprocessor, calculator)
    df = service.calc(df, daily_df, campaign_all_actual_df)
