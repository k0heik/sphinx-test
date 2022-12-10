import datetime
import pandas as pd
import numpy as np
import pytest
from spai.utils.kpi.kpi import (
    MODE_BUDGET
)


@pytest.fixture
def bid_optimiser_today():
    return datetime.datetime(2021, 4, 6)


@pytest.fixture
def bid_optimiser_input_df(bid_optimiser_today):
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range(end=bid_optimiser_today, freq="D", periods=n),
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "bidding_price": [100] * n,
            "minimum_bidding_price": [2] * n,
            "maximum_bidding_price": [1000] * n,
            "impressions": [100] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "cpc": [1.0] * n,
            "optimization_costs": [10000] * n,
            "purpose": ["SALES"] * n,
            "mode": [MODE_BUDGET] * n,
            "target_kpi": [np.nan] * n,
            "target_kpi_value": [np.nan] * n,
            "p": [0.01] * n,
            "q": [None] * n,
            "round_up_point": [1] * n,
            "remaining_days": [None] * (n - 1) + [10],
            "base_target_cost": [None] * (n - 1) + [3000],
            "target_cost": [None] * (n - 1) + [3000],
            "unit_ex_observed_C": [0.1] * n,
            "ad_observed_C_yesterday_in_month": [0.1] * n,
            "ad_weekly_ema_costs": [0.1] * n,
            "unit_weekly_ema_costs": [0.1] * n,
        }
    )
