import datetime

import pandas as pd
import pytest

from module import prepare_df


@pytest.mark.parametrize("timedelta, expected_coefficient", [
    (datetime.timedelta(days=3), [1.0, 1.0, 1.0]),
    (datetime.timedelta(days=-3), [1.0, 1.0, 1.0]),
    (datetime.timedelta(days=0), [2.5, 2.5, 2.5]),
    (datetime.timedelta(days=1), [1.0, 2.5, 2.5]),
    (datetime.timedelta(days=-1), [2.5, 2.5, 1.0]),
])
def test_prepare_df_shape_daily_budget_boost_coefficient(
    timedelta,
    expected_coefficient,
):
    target_date_from = datetime.datetime(2022, 1, 1)
    target_date_to = datetime.datetime(2022, 1, 3)
    daily_budget_boost_coefficient_df = pd.DataFrame({
        "start_date": [target_date_from + timedelta],
        "end_date": [target_date_to + timedelta],
        "coefficient": 2.5,
        "updated_at": [datetime.datetime(2022, 1, 1)],
    })

    result_df = prepare_df.shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df, target_date_from, target_date_to)

    assert list(result_df["coefficient"].values) == expected_coefficient


def test_prepare_df_shape_daily_budget_boost_coefficient_monthly_settings():
    daily_budget_boost_coefficient_df = pd.DataFrame({
        "start_date": [datetime.datetime(2022, 1, 31)],
        "end_date": [datetime.datetime(2022, 2, 1)],
        "coefficient": 2.5,
        "updated_at": [datetime.datetime(2022, 1, 1)],
    })

    result_df = prepare_df.shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df,
        datetime.datetime(2022, 1, 1),
        datetime.datetime(2022, 1, 31)
    )

    assert list(result_df["coefficient"].values) == ([1.0] * 30) + [2.5]


@pytest.mark.parametrize("timedelta, expected_coefficient", [
    (datetime.timedelta(days=-1), [1.0, 1.0, 1.0]),
    (datetime.timedelta(days=0), [2.5, 1.0, 1.0]),
    (datetime.timedelta(days=1), [1.0, 2.5, 1.0]),
    (datetime.timedelta(days=2), [1.0, 1.0, 2.5]),
    (datetime.timedelta(days=3), [1.0, 1.0, 1.0]),
])
def test_prepare_df_shape_daily_budget_boost_coefficient_onedays(
    timedelta,
    expected_coefficient,
):
    daily_budget_boost_coefficient_df_1 = pd.DataFrame({
        "start_date": [datetime.datetime(2022, 1, 1)],
        "end_date": [datetime.datetime(2022, 1, 1)],
        "coefficient": 2.5,
        "updated_at": [datetime.datetime(2022, 1, 1)],
    })
    daily_budget_boost_coefficient_df_2 = pd.DataFrame({
        "start_date": [datetime.datetime(2022, 1, 3)],
        "end_date": [datetime.datetime(2022, 1, 3)],
        "coefficient": 0.9,
        "updated_at": [datetime.datetime(2022, 1, 1)],
    })

    daily_budget_boost_coefficient_df = pd.concat([
        daily_budget_boost_coefficient_df_1,
        daily_budget_boost_coefficient_df_2,
    ])

    result_df = prepare_df.shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df,
        datetime.datetime(2021, 12, 31),
        datetime.datetime(2022, 1, 4)
    )

    assert list(result_df["coefficient"].values) == [1.0, 2.5, 1.0, 0.9, 1.0]


def test_prepare_df_shape_daily_budget_boost_coefficient_duplicate_date():
    target_date_from = datetime.datetime(2022, 1, 1)
    target_date_to = datetime.datetime(2022, 1, 3)
    daily_budget_boost_coefficient_df_1 = pd.DataFrame({
        "start_date": [target_date_from + datetime.timedelta(days=1)],
        "end_date": [target_date_to + datetime.timedelta(days=1)],
        "coefficient": 2.5,
        "updated_at": [datetime.datetime(2022, 1, 1)],
    })
    daily_budget_boost_coefficient_df_2 = pd.DataFrame({
        "start_date": [target_date_from + datetime.timedelta(days=2)],
        "end_date": [target_date_to + datetime.timedelta(days=2)],
        "coefficient": 0.9,
        "updated_at": [datetime.datetime(2022, 1, 2)],
    })

    daily_budget_boost_coefficient_df = pd.concat([
        daily_budget_boost_coefficient_df_1,
        daily_budget_boost_coefficient_df_2,
    ])

    result_df = prepare_df.shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df, target_date_from, target_date_to)

    assert list(result_df["coefficient"].values) == [1.0, 2.5, 0.9]
