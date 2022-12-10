import datetime
from unittest import mock

import numpy as np
import pandas as pd
from numpy.testing import assert_almost_equal
import pytest

from spai.utils.kpi import (
    remaining_days,
    calc_unit_weekly_cpc_for_cap,
)

from spai.utils.kpi.kpi import (
    MODE_KPI,
    MODE_BUDGET,
)
from spai.service.cap.calculator import (
    CAPCalculator,
    _TARGET_CRITERION_DAYS,
)


def today_nonfixture():
    return datetime.datetime(2021, 4, 28)


@pytest.fixture
def today():
    return today_nonfixture()


@pytest.fixture
def df(today):
    df = pd.DataFrame(
        {
            "advertising_account_id": [779, 780, 780],
            "portfolio_id": [28, 34, 34],
            "campaign_id": [462118, 462120, 462121],
            "last_costs": [711.17, 1447.9, 1447.9],
            'purpose': ['SALES', 'CONVERSION', 'CLICK'],
            'mode': [MODE_KPI, MODE_KPI, MODE_BUDGET],
            'yesterday_daily_budget': [100] * 3,
            'yesterday_costs': [100] * 3,
            'today_target_cost': [100] * 3,
            'today_noboost_target_cost': [100] * 3,
            'yesterday_target_cost': [100] * 3,
            'weight': [np.nan] * 3,
            'ideal_target_cost': [100] * 3,
            'minimum_daily_budget': [0] * 3,
            'maximum_daily_budget': [10 ** 8] * 3,
            "today_coefficient": [1.0] * 3,
            "yesterday_coefficient": [1.0] * 3,
            'date': [today] * 3,
            'C': [0.1] * 3,
            'unit_ex_observed_C': [0.1] * 3,
            'unit_weekly_ema_costs': [0.1] * 3,
            'campaign_weekly_ema_costs': [0.1] * 3,
            'campaign_observed_C_yesterday_in_month': [0.1] * 3,
        }
    )

    df['date'] = pd.to_datetime(df['date'])
    df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())
    df['remaining_days'] = remaining_days(df)
    return df


@pytest.fixture
def daily_df(df, today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df_tmp = df.copy()
        df_tmp["date"] = yesterday - datetime.timedelta(days=i)
        df_tmp["clicks"] = [4970, 7770, 683]
        df_tmp["conversions"] = [2875, 2445, 871]
        df_tmp["sales"] = [11393, 15785, 6758]
        df_tmp["costs"] = [711.17, 1447.9, 1447.9]
        dfs.append(df_tmp)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['remaining_days'] = remaining_days(daily_df)
    return daily_df


@pytest.fixture
def campaign_all_actual_df(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [779, 780, 780],
                "portfolio_id": [28, 34, 34],
                "campaign_id": [462118, 462120, 462121],
                "clicks": [4970, 7770, 683],
                "conversions": [2875, 2445, 871],
                "sales": [11393, 15785, 6758],
                "costs": [711.17, 1447.9, 1447.9],
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)
    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    return daily_df


def test_calc(today, df, daily_df, campaign_all_actual_df):
    calculator = CAPCalculator(today)
    result = calculator.calc(df, daily_df, campaign_all_actual_df)

    assert len(result) == 3
    assert not any(result['is_daily_budget_undecidable_unit'].values)


def test_used_up_budget(today, df, daily_df, campaign_all_actual_df):
    calculator = CAPCalculator(today)
    df['today_target_cost'] = 0
    df['today_noboost_target_cost'] = 0
    result = calculator.calc(df, daily_df, campaign_all_actual_df)

    assert all(result['daily_budget_upper'] == df['minimum_daily_budget'])
    assert not any(result['is_daily_budget_undecidable_unit'].values)


@pytest.mark.parametrize("zero_col", ["costs", "clicks"])
def test_unit_weekly_cpc_for_cap_is_zero(today, df, daily_df, campaign_all_actual_df, zero_col):
    calculator = CAPCalculator(today)
    campaign_all_actual_df[zero_col] = 0
    result = calculator.calc(df, daily_df, campaign_all_actual_df)

    assert all(result['daily_budget_upper'] == df['yesterday_daily_budget'])
    assert all(result['is_daily_budget_undecidable_unit'].values)


@pytest.mark.parametrize("zero_df_index", [0, 1])
def test_calc_no_target_data(today, df, daily_df, campaign_all_actual_df, zero_df_index):
    calculator = CAPCalculator(today)

    # 実際の戻り値とは項目が違うが0件テストのため似たもので代用
    clean_ret = [df, daily_df]
    clean_ret[zero_df_index] = pd.DataFrame()
    calculator._clean = mock.MagicMock(return_value=tuple(clean_ret))

    result = calculator.calc(df, daily_df, campaign_all_actual_df)

    assert result is None


def test_init_weight_normalize(today):
    daily_df = pd.DataFrame(
        {
            "advertising_account_id": [1, 1, 1],
            "portfolio_id": [1, 1, 1],
            "campaign_id": [1, 2, 3],
            "weight": [1, 1, 1]
        }
    )
    calculator = CAPCalculator(today)

    result = calculator._init_weight_normalize(daily_df, 'weight')
    expect = np.array([1 / 3] * 3)
    assert_almost_equal(result.values, expect)


@pytest.mark.parametrize("history_start_past_days, expect_decrease", [
    (1, False),
    (_TARGET_CRITERION_DAYS - 1, False),
    (_TARGET_CRITERION_DAYS, False),
    (_TARGET_CRITERION_DAYS + 1, True),
])
def test_clean(today, df, daily_df, history_start_past_days, expect_decrease):
    history_basedate = today - datetime.timedelta(days=history_start_past_days)
    df["date"] = today
    target_campain_df = df.head(1)
    daily_df.loc[
        daily_df["campaign_id"] == target_campain_df["campaign_id"].values[0], "date"
    ] = pd.date_range(end=history_basedate, freq="D", periods=len(daily_df) / len(df))

    calculator = CAPCalculator(today)

    result_df, result_daily_df = calculator._clean(df, daily_df)

    if expect_decrease:
        assert len(result_df) == len(df) - 1
        assert len(result_daily_df) == len(daily_df) * (len(df) - 1) / len(df)
    else:
        assert len(result_df) == len(df)
        assert len(result_daily_df) == len(daily_df)


def df_case_2(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [1, 1],
                "portfolio_id": [1, 1],
                "campaign_id": [1, 2],
                "clicks": [1, 1],
                "conversions": [1, 1],
                "sales": [1, 1],
                "costs": [1, 2],
                'purpose': ['SALES', 'CONVERSION'],
                'yesterday_daily_budget': [None] * 2,
                'today_target_cost': [100] * 2,
                'today_noboost_target_cost': [100] * 2,
                'yesterday_target_cost': [100] * 2,
                'weight': [np.nan] * 2,
                'ideal_target_cost': [100] * 2,
                'minimum_daily_budget': [0] * 2,
                'maximum_daily_budget': [10 ** 8] * 2,
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())

    return daily_df


def df_case_3(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [1, 1],
                "portfolio_id": [1, 1],
                "campaign_id": [1, 2],
                "clicks": [1, 0],
                "conversions": [1, 0],
                "sales": [1, 0],
                "costs": [1, 0],
                'purpose': ['SALES', 'CONVERSION'],
                'yesterday_daily_budget': [None] * 2,
                'today_target_cost': [100] * 2,
                'today_noboost_target_cost': [100] * 2,
                'yesterday_target_cost': [100] * 2,
                'weight': [np.nan] * 2,
                'ideal_target_cost': [100] * 2,
                'minimum_daily_budget': [0] * 2,
                'maximum_daily_budget': [10 ** 8] * 2,
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())

    return daily_df


def df_case_5(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [1, 1],
                "portfolio_id": [1, 1],
                "campaign_id": [1, 2],
                "clicks": [1, 1],
                "conversions": [1, 1],
                "sales": [1, 1],
                "costs": [1, 1],
                'purpose': ['SALES', 'CONVERSION'],
                'yesterday_daily_budget': [100] * 2,
                'today_target_cost': [100] * 2,
                'today_noboost_target_cost': [100] * 2,
                'yesterday_target_cost': [100] * 2,
                'weight': [1 / 2] * 2,
                'ideal_target_cost': [100] * 2,
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)

    daily_df = pd.DataFrame(
        {
            "advertising_account_id": [1],
            "portfolio_id": [1],
            "campaign_id": [3],
            "clicks": [1],
            "conversions": [1],
            "sales": [1],
            "costs": [1],
            'purpose': ['SALES'],
            'yesterday_daily_budget': [np.nan],
            'today_target_cost': [100],
            'today_noboost_target_cost': [100],
            'yesterday_target_cost': [100],
            'weight': [np.nan],
            'ideal_target_cost': [100],
            'minimum_daily_budget': [0],
            'maximum_daily_budget': [10 ** 8],
        }
    )
    daily_df["date"] = yesterday
    dfs.append(daily_df)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)

    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())

    return daily_df


def df_case_6(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [1, 1],
                "portfolio_id": [1, 1],
                "campaign_id": [1, 2],
                "clicks": [1, 1],
                "conversions": [1, 1],
                "sales": [1, 1],
                "costs": [1, 1],
                'purpose': ['SALES', 'CONVERSION'],
                'yesterday_daily_budget': [100] * 2,
                'today_target_cost': [100] * 2,
                'today_noboost_target_cost': [100] * 2,
                'yesterday_target_cost': [100] * 2,
                'weight': [1 / 2] * 2,
                'ideal_target_cost': [100] * 2,
                'minimum_daily_budget': [0] * 2,
                'maximum_daily_budget': [10 ** 8] * 2,
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)

    daily_df = pd.DataFrame(
        {
            "advertising_account_id": [1],
            "portfolio_id": [1],
            "campaign_id": [3],
            "clicks": [1],
            "conversions": [1],
            "sales": [1],
            "costs": [0],
            'purpose': ['SALES'],
            'yesterday_daily_budget': [np.nan],
            'today_target_cost': [100],
            'today_noboost_target_cost': [100],
            'yesterday_target_cost': [100],
            'weight': [np.nan],
            'ideal_target_cost': [np.nan],
            'minimum_daily_budget': [0],
            'maximum_daily_budget': [10 ** 8],
        }
    )
    daily_df["date"] = yesterday
    dfs.append(daily_df)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)

    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())

    return daily_df


def df_case_7(today):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        daily_df = pd.DataFrame(
            {
                "advertising_account_id": [1],
                "portfolio_id": [1],
                "campaign_id": [1],
                "clicks": [0],
                "conversions": [0],
                "sales": [0],
                "costs": [0],
                'purpose': ['SALES'],
                'yesterday_daily_budget': [100],
                'today_target_cost': [100],
                'today_noboost_target_cost': [100],
                'yesterday_target_cost': [100],
                'weight': [np.nan],
                'ideal_target_cost': [100],
                'minimum_daily_budget': [0],
                'maximum_daily_budget': [10 ** 8],
            }
        )
        daily_df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(daily_df)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())

    return daily_df


# 2. 重みの履歴が一切存在しない場合 and 前日の日予算なし and キャンペーンの広告費用の7日間指数移動平均 > 0 ema(costs, 7) / ema(unit_costs, 7)
# 3. 重みの履歴が一切存在しない場合 and 前日の日予算なし and キャンペーンの広告費用の7日間指数移動平均 <= 0 1 /len(campaign_ids)
# 5. 新規キャンペーン追加 and キャンペーンの広告費用の7日間指数移動平均 > 0 ema(costs, 7) / ema(unit_costs, 7)
# 6. 新規キャンペーン追加 and 4,5 以外 1 /len(campaign_ids)
# 7. 重みの履歴が一切存在しない場合 and 実績ありだが キャンペーンの広告費 unit_cpc共に0 1 /len(campaign_ids)
@pytest.mark.parametrize("case_df, expect", [
    (df_case_2(today_nonfixture()), np.array([1 / 3, 2 / 3])),
    (df_case_3(today_nonfixture()), np.array([2 / 3, 1 / 3])),
    (df_case_5(today_nonfixture()), np.array([0.34482757550535, 0.34482757550535, 0.3103448489893])),
    (df_case_6(today_nonfixture()), np.array([0.375, 0.375, 0.25])),
    (df_case_7(today_nonfixture()), np.array([1])),
])
def test_init_weight(today, case_df, expect):
    campaign_all_actual_df = case_df.copy()[[
        "advertising_account_id",
        "portfolio_id",
        "campaign_id",
        "date",
        "clicks",
        "costs",
    ]]
    calculator = CAPCalculator(today)
    case_df["clicks"] = None

    daily_df = case_df.copy()
    df = case_df.groupby(
        ["advertising_account_id", "portfolio_id", "campaign_id"], dropna=False).first().reset_index()
    df["date"] = today

    df, daily_df = calculator._clean(df, daily_df)
    init_result = calculator._init_weight(df, daily_df, campaign_all_actual_df)
    assert_almost_equal(init_result.values, expect)


@pytest.mark.parametrize("daily_budget_upper, expect_clipped", [
    (999, False),
    (1000, False),
    (1001, True),
])
def test_clip_by_max(daily_budget_upper, expect_clipped):
    df = pd.DataFrame({
        "today_target_cost": [1],
        "yesterday_target_cost": [1],
        "yesterday_daily_budget": [daily_budget_upper],
        "maximum_daily_budget": [1000],
        "minimum_daily_budget": [100],
        "daily_budget_upper": [daily_budget_upper],
        "today_coefficient": [1],
        "yesterday_coefficient": [1],
    })

    calculator = CAPCalculator(datetime.datetime(2022, 4, 1))
    result_df = calculator._clip(df.copy())

    if expect_clipped:
        expected = df["maximum_daily_budget"].values
    else:
        expected = df["daily_budget_upper"].values

    assert np.allclose(result_df["daily_budget_upper"].values,
                       expected)
    assert np.allclose(result_df["base_daily_budget_upper"].values,
                       df["daily_budget_upper"].values)


@pytest.mark.parametrize("daily_budget_upper, expect_clipped", [
    (99, True),
    (100, False),
    (101, False),
])
def test_clip_by_min(daily_budget_upper, expect_clipped):
    df = pd.DataFrame({
        "today_target_cost": 1,
        "yesterday_target_cost": [1],
        "yesterday_daily_budget": [daily_budget_upper],
        "maximum_daily_budget": [1000],
        "minimum_daily_budget": [100],
        "daily_budget_upper": [daily_budget_upper],
        "today_coefficient": [1],
        "yesterday_coefficient": [1],
    })

    calculator = CAPCalculator(datetime.datetime(2022, 4, 1))
    result_df = calculator._clip(df.copy())

    if expect_clipped:
        expected = df["minimum_daily_budget"].values
    else:
        expected = df["daily_budget_upper"].values

    assert np.allclose(result_df["daily_budget_upper"].values,
                       expected)
    assert np.allclose(result_df["base_daily_budget_upper"].values,
                       df["daily_budget_upper"].values)


@pytest.mark.parametrize(
    "max_value, window_size",
    [
        (5, 7),
        (20, 4)
    ]
)
def test_merge_max(max_value, window_size):
    today = datetime.datetime(2021, 4, 28)
    df = pd.DataFrame({
        "advertising_account_id": [1, 1],
        "portfolio_id": [None, None],
        "campaign_id": [1, 2],
    })
    df['date'] = today
    df['date'] = pd.to_datetime(df['date'])
    df["portfolio_id"] = df["portfolio_id"].astype(pd.Int64Dtype())

    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df_tmp = df.copy()
        df_tmp["date"] = yesterday - datetime.timedelta(days=i)
        df_tmp["val"] = 1
        dfs.append(df_tmp)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df.loc[-window_size:, "val"] = max_value

    calculator = CAPCalculator(today)
    result = calculator._merge_max(df, daily_df, window_size, "val", "max_val")

    assert result["max_val"].values[-1] == max_value


def get_test_df(
        today,
        yesterday_daily_budget=[1000, 1000],
        yesterday_costs=[100, 100],
        today_target_cost=[100, 100],
        weight=[0.5, 0.5],
        ideal_target_cost=[100, 100],
        today_coefficient=1.0,
        yesterday_coefficient=1.0,
):
    df = pd.DataFrame(
        {
            "advertising_account_id": [1, 1],
            "portfolio_id": [1, 1],
            "campaign_id": [1, 2],
            'yesterday_daily_budget': yesterday_daily_budget,
            'yesterday_costs': yesterday_costs,
            'today_target_cost': today_target_cost,
            'yesterday_target_cost': today_target_cost,
            'weight': weight,
            'ideal_target_cost': ideal_target_cost,
            'maximum_daily_budget': [10 ** 8] * 2,
            'date': [today] * 2,
            'today_coefficient': [today_coefficient] * 2,
            'yesterday_coefficient': [yesterday_coefficient] * 2,
            'optimization_costs': [3000] * 2,
            'mode': [MODE_KPI] * 2,
            'C': [0.1] * 2,
            'unit_ex_observed_C': [0.1] * 2,
            'unit_weekly_ema_costs': [0.1] * 2,
            'campaign_weekly_ema_costs': [0.1] * 2,
            'campaign_observed_C_yesterday_in_month': [0.1] * 2,
        }
    )

    df['date'] = pd.to_datetime(df['date'])
    df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())
    df['remaining_days'] = remaining_days(df)
    return df


def get_test_daily_df(df, today, clicks=[1, 1]):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df_tmp = df.copy()
        df_tmp["date"] = yesterday - datetime.timedelta(days=i)
        df_tmp["clicks"] = clicks
        df_tmp["costs"] = [100, 100]
        dfs.append(df_tmp)

    daily_df = pd.concat(dfs, axis=0, ignore_index=True)
    daily_df['date'] = pd.to_datetime(daily_df['date'])
    daily_df['remaining_days'] = remaining_days(daily_df)
    return daily_df


def get_test_all_campaign_actual_df(df, today, clicks=[1, 1]):
    yesterday = today - datetime.timedelta(days=1)
    dfs = list()
    for i in range(28):
        df_tmp = df.copy()
        df_tmp["date"] = yesterday - datetime.timedelta(days=i)
        df_tmp["clicks"] = clicks
        df_tmp["costs"] = [100, 100]
        dfs.append(df_tmp)
    all_campaign_actual_df = pd.concat(dfs, axis=0, ignore_index=True)
    return all_campaign_actual_df


@pytest.mark.parametrize(
    "has_potential, is_end_of_the_month, is_today_cost_bigger_potential, unit_weekly_cpc,"
    "upper_ratio_bounds, upper_ratio_bounds_not_potential, upper_ratio_bounds_cpc_not_potential",
    [
        (True, False, True, None, 2.0, 2.0, 2.0),
        (True, True, False, None, 2.0, 2.0, 2.0),
        (True, False, False, None, 2.0, 2.0, 2.0),
        (True, False, False, None, 2.0, 2.0, 2.0),
        (False, False, False, 100, 2.0, 2.0, 2.0),
        (False, True, False, 100, 2.0, 2.0, 2.0),
        (False, False, False, 0, 2.0, 2.0, 2.0),
        (False, True, False, 0, 2.0, 2.0, 2.0),
        (False, True, False, 0, 2.0, 2.0, 2.0),
    ]
)
def test_calc_daily_budget_upper(
        has_potential, is_end_of_the_month, is_today_cost_bigger_potential,
        unit_weekly_cpc, upper_ratio_bounds,
        upper_ratio_bounds_not_potential, upper_ratio_bounds_cpc_not_potential):

    if is_end_of_the_month:
        today = datetime.datetime(2021, 4, 30)
    else:
        today = datetime.datetime(2021, 4, 28)

    df = get_test_df(today)

    df["ideal_target_cost"] = 100
    df["weight"] = 0.5

    if has_potential:
        df["yesterday_daily_budget"] = 100
        if is_today_cost_bigger_potential:
            df["today_target_cost"] = 1000
            df["yesterday_costs"] = 350
        else:
            df["today_target_cost"] = 100
            df["yesterday_costs"] = 70
    else:
        df["yesterday_daily_budget"] = 1000
        df["today_target_cost"] = 1500
        df["yesterday_costs"] = 100

    df["today_noboost_target_cost"] = df["today_target_cost"]

    daily_df = get_test_daily_df(df, today)
    all_campaign_actual_df = get_test_all_campaign_actual_df(df, today)
    df["unit_weekly_cpc_for_cap"] = calc_unit_weekly_cpc_for_cap(all_campaign_actual_df, today)

    if unit_weekly_cpc is not None and unit_weekly_cpc == 0:
        daily_df["clicks"] = 0
        all_campaign_actual_df["clicks"] = 0
    else:
        daily_df["clicks"] = 1
        all_campaign_actual_df["clicks"] = 1

    calculator = CAPCalculator(today)
    result_df = calculator._calc_daily_budget_upper(
        df.copy(), daily_df.copy(),
        upper_ratio_bounds=upper_ratio_bounds,
        upper_ratio_bounds_not_potential=upper_ratio_bounds_not_potential,
        upper_ratio_bounds_cpc_not_potential=upper_ratio_bounds_cpc_not_potential
    )

    if has_potential:
        assert np.allclose(
            result_df["has_potential"].values,
            np.array([True, True]))
        expected = np.array(
            df["today_target_cost"].values) * np.array(df["weight"].values)
    else:
        assert np.allclose(
            result_df["has_potential"].values,
            np.array([False, False]))

        expected = np.array(df["yesterday_costs"].values * upper_ratio_bounds_not_potential)

    assert np.allclose(
        result_df["daily_budget_upper"].values, expected)


@pytest.mark.parametrize('is_month_first_day', [True, False])
@pytest.mark.parametrize(
    "today_coefficient, yesterday_coefficient, yesterday_target_cost, yesterday_daily_budget_value",
    [
        (1.0, 1.0, 0, 33),
        (1.0, 1.0, 500, 33),
        (2.0, 1.0, 500, 3),
    ]
)
def test_clip_beginning_of_month(
    is_month_first_day, today_coefficient, yesterday_coefficient, yesterday_target_cost, yesterday_daily_budget_value
):

    if is_month_first_day:
        today = datetime.datetime(2021, 4, 1)
    else:
        today = datetime.datetime(2021, 4, 10)

    ideal_target_cost = [50, 50]
    weight = [0.5, 0.5]
    yesterday_daily_budget = [yesterday_daily_budget_value] * 2
    today_target_cost = [100, 100]
    yesterday_costs = [70, 70]
    upper_ratio_bounds = 3.0

    df = get_test_df(
        today, yesterday_daily_budget, yesterday_costs,
        today_target_cost, weight, ideal_target_cost,
        today_coefficient, yesterday_coefficient,
    )
    df["daily_budget_upper"] = 10 ** 10
    df['yesterday_target_cost'] = yesterday_target_cost
    df["minimum_daily_budget"] = 0
    df["maximum_daily_budget"] = 10 ** 10

    calculator = CAPCalculator(today)
    result_df = calculator._clip(df, upper_ratio_bounds=upper_ratio_bounds)

    if yesterday_target_cost == 0 and is_month_first_day:
        expected = df["optimization_costs"].values[0] / 30
    else:
        expected = upper_ratio_bounds * np.array(yesterday_daily_budget)

    assert np.allclose(result_df["daily_budget_upper"].values,
                       expected)


@pytest.mark.parametrize('is_month_first_day', [True, False])
@pytest.mark.parametrize('mode, is_good_unit_performence, is_costs_excessive, is_expect_check_clip_upper', [
    (MODE_KPI, True, True, False),
    (MODE_KPI, False, True, True),
    (MODE_KPI, True, False, False),
    (MODE_KPI, False, False, True),
    (MODE_BUDGET, True, True, False),
    (MODE_BUDGET, False, True, True),
    (MODE_BUDGET, True, False, False),
    (MODE_BUDGET, False, False, False),
])
@pytest.mark.parametrize('is_high_percentage_in_cost', [True, False])
@pytest.mark.parametrize('is_bad_performance', [True, False])
@pytest.mark.parametrize('is_kpi_purpose_is_none', [True, False])
def test_clip_bad_performance(
    is_month_first_day,
    mode, is_good_unit_performence, is_costs_excessive, is_expect_check_clip_upper,
    is_high_percentage_in_cost, is_bad_performance, is_kpi_purpose_is_none,
):
    if is_month_first_day:
        today = datetime.datetime(2021, 4, 1)
    else:
        today = datetime.datetime(2021, 4, 2)

    today_daily_budget = 45
    yesterday_daily_budget = today_daily_budget - 1

    df = get_test_df(today, [yesterday_daily_budget] * 2)

    df["daily_budget_upper"] = today_daily_budget

    df["mode"] = mode

    if is_good_unit_performence:
        df["unit_ex_observed_C"] = df["C"]
    else:
        df["unit_ex_observed_C"] = df["C"] + df["C"] / 10

    if is_kpi_purpose_is_none:
        df["C"] = None

    if is_costs_excessive:
        df["today_target_cost"] = df["ideal_target_cost"] - df["ideal_target_cost"] / 10
    else:
        df["today_target_cost"] = df["ideal_target_cost"]

    if is_high_percentage_in_cost:
        df["campaign_weekly_ema_costs"] = df["unit_weekly_ema_costs"] / 9
    else:
        df["campaign_weekly_ema_costs"] = df["unit_weekly_ema_costs"] / 10

    if is_bad_performance:
        df["campaign_observed_C_yesterday_in_month"] = df["C"] + df["C"] / 10

    calculator = CAPCalculator(today)
    result_df = calculator._clip_bad_performance(df)

    if (
        not is_month_first_day
        and is_expect_check_clip_upper
        and is_high_percentage_in_cost
        and is_bad_performance
        and not is_kpi_purpose_is_none
    ):
        assert all(result_df["daily_budget_upper"].values == yesterday_daily_budget)
    else:
        assert all(result_df["daily_budget_upper"].values == today_daily_budget)
