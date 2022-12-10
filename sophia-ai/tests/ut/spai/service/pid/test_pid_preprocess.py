import numpy as np
import pandas as pd
import pytest

from spai.service.pid.preprocess import PIDPreprocessor


@pytest.fixture
def df():
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "ad_group_id": [1] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range("2021-04-01", f"2021-04-{n:>02}"),
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
            "unit_ex_observed_C": [0.1] + [None] * (n - 1),
        }
    )


def test_fillna(df):
    preprocessor = PIDPreprocessor()
    # 0埋め
    cols = ["impressions", "clicks", "costs"]
    df[cols] = None
    df = preprocessor._fillna(df)
    np.testing.assert_array_equal(
        df[cols].values, np.zeros_like(df[cols].values)
    )


def test_transform(df):
    preprocessor = PIDPreprocessor()
    # 適当なデータで通ることを確認
    df = preprocessor.transform(df)
    # 空の場合でもcolumnsを作成
    empty_df = pd.DataFrame()
    output = preprocessor.transform(empty_df)
    assert all(col in df.columns for col in output)
    assert all(col in output.columns for col in df.columns)
