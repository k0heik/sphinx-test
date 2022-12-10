import datetime
import random
import numpy as np
import pandas as pd
import pytest

from spai.service.pid.calculator import PIDCalculator
from spai.optim.models import KPI, Purpose, Mode, \
    Settings, State


@pytest.fixture
def today():
    return datetime.datetime(2021, 4, 23)


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


@pytest.mark.parametrize("is_error", [True, False])
@pytest.mark.parametrize("is_updated", [True, False])
@pytest.mark.parametrize("is_pid_initialized", [True, False])
@pytest.mark.parametrize("valid_ads_num", [0, 10])
def test_get_row(is_error, is_updated, is_pid_initialized, valid_ads_num):
    calculator = PIDCalculator(today)
    obs_kpi = random.random()
    s = Settings(KPI.ROAS, Purpose.SALES, Mode.BUDGET, 1000, 1000, KPI.ROAS, False)
    p = State(output=1.0, original_output=1.0)
    q = State(output=2.0)
    pre_reupdate_p = State(output=3.0, original_output=1.0)
    pre_reupdate_q = State(output=4.0, original_output=1.0)
    row = calculator._get_row(
        s, p, q, pre_reupdate_p, pre_reupdate_q, is_error, is_updated, is_pid_initialized, obs_kpi, valid_ads_num)
    assert row["p"] == pytest.approx(p.output)
    assert row["origin_p"] == pytest.approx(p.original_output)
    assert row["error"] == is_error
    assert row["is_updated"] == is_updated
    assert row["is_pid_initialized"] == is_pid_initialized
    assert row["obs_kpi"] == obs_kpi
    assert row["valid_ads_num"] == valid_ads_num


def test_is_abnormal():
    calculator = PIDCalculator(today)
    p = State()
    q = State()
    p.output = 1.0
    assert not calculator._is_abnormal(p, q)
    p.output = 0.0
    assert calculator._is_abnormal(p, q)


def preprocessed_df(contain_nan_bid, all_bid_nan):
    n = 3
    bids = [np.nan if all_bid_nan else 1] * 1 + [np.nan] * 2
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "ad_type": ["keyword", "keyword", "keyword"],
            "ad_id": [1] * n,
            "date": pd.date_range("2021-04-21", "2021-04-23"),
            "impressions": [5753217, 1911269, 1208205],
            "clicks": [184873, 48562, 31404],
            "conversions": [82538, 25161, 16015],
            "sales": [443125.21, 142456.91, 77885.87],
            "costs": [36347.13, 9712.54, 6051.88],
            "bidding_price": bids if contain_nan_bid else [1.0] * n,
            "ctr": [1.0] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "cpc": [1.0] * n,
            "purpose": ["SALES"] * n,
            "mode": ["KPI", "BUDGET", "KPI"],
            "target_kpi": ["ROAS"] * n,
            "target_cost": [62500.0, 31250.0, 12500.0],
            "base_target_cost": [16666.667, 8333.333, 3333.333],
            "target_kpi_value": [1.0, 10.0, 3.0],
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "p": [np.nan] * n,
            "q": [np.nan] * n,
            "p_kp": [np.nan] * n,
            "p_ki": [np.nan] * n,
            "p_kd": [np.nan] * n,
            "p_error": [0.0] * n,
            "p_sum_error": [0.0] * n,
            "q_kp": [np.nan] * n,
            "q_ki": [np.nan] * n,
            "q_kd": [np.nan] * n,
            "q_error": [0.0] * n,
            "q_sum_error": [0.0] * n,
            "not_ml_applied_days": [3] * n,
            "yesterday_target_kpi": ["ROAS"] * n,
            "unit_ex_observed_C": [0.1] * n,
        }
    )


@pytest.mark.parametrize("contain_nan_bid, all_bid_nan", [
    (False, False),
    (True, False),
    (True, True),
])
@pytest.mark.parametrize("zero_target_cost, pq_value_nan", [
    (True, True),
    (True, False),
    (False, True),
])
def test_get_updated_result(
    today,
    campaign_all_actual_df,
    contain_nan_bid,
    all_bid_nan,
    zero_target_cost,
    pq_value_nan
):
    df = preprocessed_df(contain_nan_bid, all_bid_nan)
    if zero_target_cost:
        df.loc[len(df) - 1, "target_cost"] = 0
        if not pq_value_nan:
            df["p"] = random.random()
            df["q"] = random.random()

    calculator = PIDCalculator(today)
    res_df = calculator.calc(df, campaign_all_actual_df)
    if all_bid_nan or (contain_nan_bid and not zero_target_cost):
        assert res_df is None
    else:
        assert len(res_df) == 1

        if zero_target_cost:
            assert all(res_df["is_skip_pid_calc_state"])
            if pq_value_nan:
                assert res_df["p"].isnull().all()
                assert res_df["q"].isnull().all()
            else:
                assert res_df["p"].values[0] == df["p"].values[-1]
                assert res_df["q"].values[0] == df["q"].values[-1]
        else:
            assert not any(res_df["is_skip_pid_calc_state"])
            assert not res_df["p"].isnull().any()
            assert not res_df["q"].isnull().any()


def test_get_invalid_ads_result(campaign_all_actual_df, today):
    calculator = PIDCalculator(today)
    df = pd.DataFrame(
        {
            "advertising_account_id": [2, 2, 2],
            "portfolio_id": [-1, -1, -1],
            "ad_type": ["keyword", "keyword", "keyword"],
            "ad_id": [1, 1, 1],
            "date": ["2021-04-21", "2021-04-22", "2021-04-23"],
            "impressions": [5753217, 1911269, 1208205],
            "clicks": [184873, 48562, 31404],
            "conversions": [0] * 3,
            "sales": [0] * 3,
            "costs": [36347.13, 9712.54, 6051.88],
            "bidding_price": [1.0] * 3,
            "ctr": [0] * 3,
            "cvr": [0] * 3,
            "rpc": [0] * 3,
            "cpc": [0] * 3,
            "purpose": ["SALES", "SALES", "SALES"],
            "mode": ["KPI", "BUDGET", "KPI"],
            "target_kpi": ["ROAS", "ROAS", "ROAS"],
            "target_cost": [62500.0, 31250.0, 12500.0],
            "base_target_cost": [16666.667, 8333.333, 3333.333],
            "target_kpi_value": [1.0, 10.0, 3.0],
            "is_enabled_bidding_auto_adjustment": [True] * 3,
            "p": [np.nan, np.nan, np.nan],
            "q": [np.nan, np.nan, np.nan],
            "p_kp": [np.nan, np.nan, np.nan],
            "p_ki": [np.nan, np.nan, np.nan],
            "p_kd": [np.nan, np.nan, np.nan],
            "p_error": [0.0, 0.0, 0.0],
            "p_sum_error": [0.0, 0.0, 0.0],
            "q_kp": [np.nan, np.nan, np.nan],
            "q_ki": [np.nan, np.nan, np.nan],
            "q_kd": [np.nan, np.nan, np.nan],
            "q_error": [0.0, 0.0, 0.0],
            "q_sum_error": [0.0, 0.0, 0.0],
            "not_ml_applied_days": [3, 3, 3],
            "yesterday_target_kpi": ["ROAS", "ROAS", "ROAS"],
            "unit_ex_observed_C": [0.1, 0.1, 0.1],
        }
    )
    res = calculator.calc(df, campaign_all_actual_df)
    assert len(res) == 1
