import datetime

import pandas as pd
import pytest

from spai.service.pid.config import ML_LOOKUP_DAYS


@pytest.fixture
def pid_controller_input_df(common_data_today):
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range(end=common_data_today, freq="D", periods=n),
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
            "remaining_days": [3] * n,
            "target_cost": [100] * n,
            "base_target_cost": [100] * n,
            "yesterday_target_kpi": ["ROAS"] * n,
            "unit_ex_observed_C": [0.1] * n,
        }
    )


@pytest.fixture
def ml_applied_history_df(common_data_yesterday):
    date_from = common_data_yesterday - datetime.timedelta(days=1)
    return pd.DataFrame(
        {"date": pd.date_range(end=date_from, freq="D", periods=ML_LOOKUP_DAYS - 1)}
    )
