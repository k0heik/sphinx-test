import numpy as np
import pandas as pd
import pytest

from spai.utils.kpi import (
    target_kpi,
    target_kpi_value,
    adjust_roas_target,
    purpose,
    mode,
)


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
            "not_ml_applied_days": [3] * n
        }
    )


def test_target_kpi(df):
    # 0: roas, 1: cpa, 2:cpc, else: nan
    df["optimization_purpose"] = [0, 1, 2, 3, None, "cpa"]
    tkpi = target_kpi(df)
    np.testing.assert_array_equal(
        tkpi.values, np.array(["ROAS", "CPA", "CPC", "NULL", "NULL", "NULL"])
    )
    # 値のnan
    df["optimization_purpose_value"] = np.nan
    tkpi = target_kpi(df)
    np.testing.assert_array_equal(
        tkpi.values, np.array(["NULL", "NULL", "NULL", "NULL", "NULL", "NULL"])
    )


def test_target_kpi_value(df):
    # 値のnan
    df["optimization_purpose"] = [0, 1, 2, 3, None, "cpa"]
    df["optimization_purpose_value"] = 1
    df["target_kpi"] = target_kpi(df)
    tkpi_v = target_kpi_value(df)
    np.testing.assert_array_equal(
        tkpi_v.values, np.array([1, 1, 1, 1, np.nan, 1])
    )


def test_adjust_roas_target(df):
    # roasのみ百分率表記なので比率に戻す
    df["optimization_purpose"] = [0, 1, 2, 0, 1, 2]
    df["optimization_purpose_value"] = [100, 100, 100, 10, 10, 10]
    df["target_kpi"] = target_kpi(df)
    df["target_kpi_value"] = target_kpi_value(df)
    tkpi_v = adjust_roas_target(df["target_kpi"], df["target_kpi_value"])
    np.testing.assert_array_equal(tkpi_v, np.array([1, 100, 100, 0.1, 10, 10]))


def test_purpose(df):
    # 0: sales, 1: conversion, 2:click, else: sales(default)
    df["optimization_purpose"] = [0, 1, 2, None, 3, "null"]
    tkpi = target_kpi(df)
    output = purpose(tkpi)
    np.testing.assert_array_equal(
        output,
        np.array(["SALES", "CONVERSION", "CLICK", "SALES", "SALES", "SALES"]),
    )


def test_mode(df):
    # budget -> 予算優先, goal -> KPI優先
    df["optimization_priority_mode_type"] = ["budget"] * 3 + ["goal"] * 3
    output = mode(df)
    np.testing.assert_array_equal(
        output.values, np.array(["BUDGET"] * 3 + ["KPI"] * 3)
    )
