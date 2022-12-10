import numpy as np
import pandas as pd
import pytest

from spai.utils.kpi import (
    remaining_days,
    target_cost,
    used_cost,
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
            "ctr": [1.0] * n,
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


def _generate_cost_test_data(
    df,
    optimization_costs,
    month_in_days,
    portfolio_id,
    remaining_days_value,
    remaining_cost,
    target_date,
    add_df_slice
):
    df["portfolio_id"] = portfolio_id
    df["portfolio_id"] = df["portfolio_id"].astype(pd.Int64Dtype())
    df["optimization_costs"] = optimization_costs
    if remaining_days_value == month_in_days:
        df["sum_costs"] = 99999
    else:
        df["sum_costs"] = (df["optimization_costs"] - remaining_cost) / 5
    df["date"] = pd.date_range(periods=6, freq="D", end=target_date)

    base_campaign_id = df["campaign_id"].values[0]
    add_campaign_id = base_campaign_id + 1

    tmp_df = df.iloc[add_df_slice:, :].copy()
    tmp_df.loc[:, "campaign_id"] = add_campaign_id

    return pd.concat([df, tmp_df]), base_campaign_id, add_campaign_id


def test_remaining_days(df):
    df["date"] = [
        "2021-04-01",
        "2021-05-01",
        "2021-04-30",
        "2021-02-20",
        "2021-05-16",
        "2021-04-11",
    ]
    df["date"] = pd.to_datetime(df["date"])
    output = remaining_days(df)
    np.testing.assert_array_equal(
        output.values, np.array([30, 31, 1, 9, 16, 20])
    )


@pytest.mark.parametrize(
    "target_date, remaining_days_value", [
        ("2021-04-06", 25),
        ("2021-04-29", 2),
        ("2021-04-30", 1),
        ("2021-04-01", 30),
    ]
)
@pytest.mark.parametrize("portfolio_id", [1, None])
@pytest.mark.parametrize("add_df_slice", [-5, -3])
@pytest.mark.parametrize("nan_range", [0, 1, 3])
def test_used_cost(df, portfolio_id, add_df_slice, nan_range, target_date, remaining_days_value):
    optimization_costs = 10000
    month_in_days = 30

    df, base_campaign_id, add_campaign_id = _generate_cost_test_data(
        df,
        optimization_costs,
        month_in_days,
        portfolio_id,
        remaining_days_value,
        1,
        target_date,
        add_df_slice,
    )

    daily_sum_costs = 100
    df["sum_costs"] = daily_sum_costs
    if nan_range > 0:
        df.loc[
            df["date"].isin(pd.date_range(periods=nan_range, freq="D", end=target_date).values),
            "sum_costs"
        ] = np.nan

    df = used_cost(df)

    np.testing.assert_array_equal(
        df[df["campaign_id"] == base_campaign_id]["used_costs"].values[-1],
        daily_sum_costs * max(min(5, month_in_days - remaining_days_value) - max(nan_range - 1, 0), 0)
    )
    np.testing.assert_array_equal(
        df[df["campaign_id"] == base_campaign_id].iloc[add_df_slice:, :]["used_costs"].values,
        df[df["campaign_id"] == add_campaign_id]["used_costs"].values
    )


@pytest.mark.parametrize(
    "target_date, remaining_days_value, remaining_cost, is_expect_under_ideal_pace", [
        ("2021-04-06", 25, 5000, False),
        ("2021-04-29", 2, 5000, True),
        ("2021-04-30", 1, 5000, False),
        ("2021-04-29", 2, 0, False),
        ("2021-04-30", 1, 0, False),
        ("2021-04-01", 30, 10000, False),
    ]
)
@pytest.mark.parametrize("portfolio_id", [1, None])
def test_target_cost(
    df,
    target_date,
    remaining_days_value,
    remaining_cost,
    is_expect_under_ideal_pace,
    portfolio_id,
):
    optimization_costs = 10000
    month_in_days = 30
    add_df_slice = -2

    df, _, _ = _generate_cost_test_data(
        df,
        optimization_costs,
        month_in_days,
        portfolio_id,
        remaining_days_value,
        remaining_cost,
        target_date,
        add_df_slice,
    )

    monthly_coefficient_df = pd.DataFrame({
        "date": pd.date_range("2021-04-01", "2021-04-30"),
        "coefficient": [1.0] * 30,
    })

    df = used_cost(df)

    with pytest.raises(ValueError):
        target_cost(df, monthly_coefficient_df)

    df["remaining_days"] = remaining_days(df)

    result_df = target_cost(df, monthly_coefficient_df)

    assert result_df["ideal_target_cost"].values[-1] == optimization_costs / month_in_days
    assert result_df["allocation_target_cost"].values[-1] == remaining_cost / remaining_days_value

    if is_expect_under_ideal_pace:
        assert pytest.approx(result_df["target_cost"].values[-1]) == (
            2 * (remaining_cost / remaining_days_value) - (optimization_costs / month_in_days)
        )
    else:
        assert pytest.approx(result_df["target_cost"].values[-1]) == remaining_cost / remaining_days_value

    # today boost coefficient
    assert all(result_df["target_cost"].fillna(-1) == result_df["noboost_target_cost"].fillna(-1))


@pytest.mark.parametrize(
    ("boost_start, boost_end, today_coefficient, remaining_coefficient,"
     "month_in_coefficient, remaining_cost, is_expect_under_ideal_pace"), [
        ("2021-04-25", "2021-04-25", 2.5, 7.5, 31.5, 750, False),
        ("2021-04-26", "2021-04-26", 1, 7.5, 31.5, 750, False),
        ("2021-04-06", "2021-04-06", 1, 6, 31.5, 750, True),
        ("2021-04-26", "2021-04-30", 1, 13.5, 37.5, 1350, False),
        ("2021-04-26", "2021-04-30", 1, 13.5, 37.5, 1351, True),
    ]
)
@pytest.mark.parametrize("portfolio_id", [1, None])
def test_target_cost_coefficient(
    boost_start,
    boost_end,
    today_coefficient,
    remaining_coefficient,
    month_in_coefficient,
    remaining_cost,
    is_expect_under_ideal_pace,
    portfolio_id,
):
    optimization_costs = 100 * month_in_coefficient
    df = pd.DataFrame({
        "advertising_account_id": [1],
        "portfolio_id": [portfolio_id],
        "date": [pd.to_datetime("2021-04-25")],
        "optimization_costs": [optimization_costs],
        "used_costs": [optimization_costs - remaining_cost],
    })
    df["remaining_days"] = 6
    monthly_coefficient_df = pd.DataFrame({
        "date": pd.date_range("2021-04-01", "2021-04-30"),
        "coefficient": [1.0] * 30,
    })
    for date in pd.date_range(boost_start, boost_end):
        monthly_coefficient_df.loc[monthly_coefficient_df["date"] == date, "coefficient"] = 2.5

    result_df = target_cost(df, monthly_coefficient_df)

    assert result_df["ideal_target_cost"].values[-1] == \
        today_coefficient * optimization_costs / month_in_coefficient
    assert result_df["allocation_target_cost"].values[-1] == \
        today_coefficient * remaining_cost / remaining_coefficient

    if is_expect_under_ideal_pace:
        assert pytest.approx(result_df["target_cost"].values[-1]) == (
            today_coefficient * 2 * (remaining_cost / remaining_coefficient)
            - (optimization_costs / month_in_coefficient)
        )
    else:
        assert pytest.approx(result_df["target_cost"].values[-1]) == \
            today_coefficient * remaining_cost / remaining_coefficient
