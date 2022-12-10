import numpy as np
import pandas as pd
import pytest

from spai.service.bid.preprocess import BIDPreprocessor
from spai.service.bid.calculator import BIDCalculator
from spai.service.bid.service import BIDCalculationService


@pytest.fixture
def df():
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [1] * n,
            "ad_group_id": [None] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range("2021-04-01", f"2021-04-{n:>02}"),
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "bidding_price": [100] * n,
            "minimum_bidding_price": [2] * n,
            "maximum_bidding_price": [1000] * n,
            "impressions": [100] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
            "ctr": [1.0] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "cpc": [1.0] * n,
            "optimization_costs": [10000] * n,
            "purpose": ["SALES"] * n,
            "target_kpi": [np.nan] * n,
            "target_kpi_value": [np.nan] * n,
            "p": [0.01] * n,
            "q": [None] * n,
            "round_up_point": [1] * n,
            "remaining_days": [None] * (n - 1) + [10],
            "base_target_cost": [None] * (n - 1) + [3000],
            "target_cost": [None] * (n - 1) + [3000],
        }
    )


@pytest.fixture
def campaign_all_actual_df(df):
    return df[[
        "advertising_account_id",
        "portfolio_id",
        "campaign_id",
        "date",
        "impressions",
        "clicks",
        "costs",
        "conversions",
        "sales",
    ]]


def test_calc(df, campaign_all_actual_df):
    preprocessor = BIDPreprocessor()
    calculator = BIDCalculator(df["date"].max())
    service = BIDCalculationService(preprocessor, calculator)

    df = service.calc(df, campaign_all_actual_df, "")
