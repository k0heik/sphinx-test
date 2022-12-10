import datetime
import pandas as pd
import pytest

from spai.service.pid.preprocess import PIDPreprocessor
from spai.service.pid.calculator import PIDCalculator
from spai.service.pid.service import PIDCalculationService


@pytest.fixture
def today():
    return datetime.datetime(2021, 4, 6)


@pytest.fixture
def df(today):
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "ad_group_id": [1] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range(today - datetime.timedelta(days=n-1), today),
            "bidding_price": [100] * n,
            "impressions": [100] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
            "optimization_costs": [10000] * n,
            "optimization_priority_mode_type": ["goal"] * n,
            "optimization_purpose": [0] * n,
            "optimization_purpose_value": [1] * n,
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "ctr": [1.0] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "cpc": [1.0] * n,
            "p": [None] * n,
            "q": [None] * n,
            "p_kp": [None] * n,
            "p_ki": [None] * n,
            "p_kd": [None] * n,
            "p_error": [None] * n,
            "p_sum_error": [None] * n,
            "q_kp": [None] * n,
            "q_ki": [None] * n,
            "q_kd": [None] * n,
            "q_error": [None] * n,
            "q_sum_error": [None] * n,
            "weekly_clicks": [100] * n,
            "monthly_conversions": [100] * n,
            "monthly_sales": [100] * n,
            "sum_costs": [100] * n,
            "not_ml_applied_days": [3] * n,
            "target_kpi": ["ROAS"] * n,
            "target_kpi_value": [1] * n,
            "mode": ["KPI"] * n,
            "purpose": ["SALES"] * n,
            "remaining_days": [20] + [None] * (n - 1),
            "target_cost": [100] + [None] * (n - 1),
            "base_target_cost": [100] + [None] * (n - 1),
            "yesterday_target_kpi": ["ROAS"] * n,
            "unit_ex_observed_C": [1] + [None] * (n - 1),
        }
    )


@pytest.fixture
def campaign_all_actual_df(today):
    n = 6
    return pd.DataFrame(
        {
            "campaign_id": [1] * n,
            "date": pd.date_range(today - datetime.timedelta(days=n-1), today),
            "impressions": [100] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
        }
    )


def test_transform(df, campaign_all_actual_df, today):
    preprocessor = PIDPreprocessor()
    calculator = PIDCalculator(today)
    service = PIDCalculationService(preprocessor, calculator)

    df = service.calc(df, campaign_all_actual_df)
