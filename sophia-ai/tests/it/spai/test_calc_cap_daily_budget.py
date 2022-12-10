import datetime
import pytest
import numpy as np
import pandas as pd
import os

from spai.optim.utils import weighted_ma
from spai.utils.kpi import (
    remaining_days,
)
from spai.utils.kpi.kpi import MODE_KPI
from spai.service.cap.config import UNIT_PK_COLS
from spai.service.cap.calculator import CAPCalculator


BASE_DIR = os.path.dirname(__file__)


@pytest.fixture
def today():
    return datetime.datetime(2021, 4, 27)


@pytest.fixture
def yesterday(today):
    return today - datetime.timedelta(days=1)


@pytest.fixture
def calculator(today):
    return CAPCalculator(today)


@pytest.fixture
def df(today):
    df = pd.DataFrame(
        {
            "advertising_account_id": [779, 780, 780],
            "portfolio_id": [28, 34, 34],
            "campaign_id": [462118, 462120, 462121],
            "yesterday_costs": [711.17, 1447.9, 1447.9],
            "purpose": ['SALES', 'CONVERSION', 'CLICK'],
            "optimization_costs": [100000, 50000, 10000],
            "optimization_purpose": [0, 0, 0],
            "yesterday_daily_budget": [1000] * 3,
            "master_budget": [1000] * 3,
            "used_costs": [20000.1, 20000.1, 20000.1],
            "weight": [np.nan, np.nan, np.nan],
            'today_target_cost': [1000] * 3,
            'yesterday_target_cost': [1000] * 3,
            'ideal_target_cost': [100] * 3,
            'minimum_daily_budget': [0] * 3,
            'maximum_daily_budget': [10 ** 8] * 3,
            'date': [today] * 3,
            "today_coefficient": [1.0] * 3,
            "yesterday_coefficient": [1.0] * 3,
        }
    )
    df['date'] = pd.to_datetime(df['date'])
    df['remaining_days'] = remaining_days(df)
    df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())

    return df


@pytest.fixture
def daily_df(yesterday):
    dfs = list()
    for i in range(28):
        df = pd.DataFrame(
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
        df["date"] = yesterday - datetime.timedelta(days=i)
        dfs.append(df)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df['remaining_days'] = remaining_days(df)
    df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())

    return df


def read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=['date'])
    df["yesterday_daily_budget"] = np.nan
    df['minimum_daily_budget'] = 0
    df['maximum_daily_budget'] = 10 ** 8

    today = pd.to_datetime(df["date"]).max() + datetime.timedelta(days=1)
    daily_df = df.copy()
    df = df[df["date"] == df["date"].max()]
    df["date"] = today
    df = df.rename(columns={
        "costs": "yesterday_costs",
    })

    calculator = CAPCalculator(today)

    aa, _ = calculator._clean(df, daily_df)
    return calculator, calculator._clean(df, daily_df)


def test_init_weight_all_null():
    csv_path = os.path.join(BASE_DIR, 'test_init_weight_all_null.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    weight = calculator._init_weight(df, daily_df, daily_df).values
    assert all(weight[i] == pytest.approx(1 / len(df))
               for i in range(len(df)))


def test_init_weight_with_historical_performance():
    csv_path = os.path.join(BASE_DIR, 'test_init_weight_with_historical_performance.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    weight = calculator._init_weight(df, daily_df, daily_df).values
    np.testing.assert_almost_equal(weight, np.array([1 / 6, 2 / 6, 3 / 6]))


def test_init_weight_with_historical_performance_zero_cost():
    csv_path = os.path.join(BASE_DIR, 'test_init_weight_with_historical_performance_zero_cost.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    weight = calculator._init_weight(df, daily_df, daily_df).values
    np.testing.assert_almost_equal(weight, np.array([5 / 20, 6 / 20, 9 / 20]))


def test_init_weight_all_filled(calculator):
    csv_path = os.path.join(BASE_DIR, 'test_init_weight_all_filled.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    prev_weight = df['weight'].values
    weight = calculator._init_weight(df, daily_df, daily_df).values
    assert all(weight[i] == pytest.approx(prev_weight[i])
               for i in range(len(df)))


def test_init_weight_removed():
    csv_path = os.path.join(BASE_DIR, 'test_init_weight_removed.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df).values
    assert all(g['weight'].sum() == pytest.approx(1.0)
               for _, g in df.groupby(UNIT_PK_COLS))


def test_calc_value_of_campaign_all_click():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign_all_click.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    # value = ema(clicks, 7)
    kpi = df.groupby([
        "advertising_account_id",
        "portfolio_id",
        "campaign_id"],
        dropna=False
    ).apply(lambda x:
            weighted_ma(
                x.sort_values("date")["clicks"],
                7
            )
            ).values
    value = calculator._calc_value_of_campaign(df, daily_df).values
    assert all(value[i] == pytest.approx(kpi[i])
               for i in range(len(df)))


def test_calc_value_of_campaign_all_conversion():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign_all_conversion.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    # value = ema(conversions, 28)
    kpi = df.groupby([
        "advertising_account_id",
        "portfolio_id",
        "campaign_id"],
        dropna=False
    ).apply(lambda x:
            weighted_ma(
                x.sort_values("date")["conversions"],
                28
            )
            ).values
    value = calculator._calc_value_of_campaign(df, daily_df).values
    assert all(value[i] == pytest.approx(kpi[i])
               for i in range(len(df)))


def test_calc_value_of_campaign_all_sales():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign_all_sales.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    kpi = df.groupby([
        "advertising_account_id",
        "portfolio_id",
        "campaign_id"],
        dropna=False
    ).apply(lambda x:
            weighted_ma(
                x.sort_values("date")["sales"],
                28
            )
            ).values
    value = calculator._calc_value_of_campaign(df, daily_df).values
    assert all(value[i] == pytest.approx(kpi[i])
               for i in range(len(df)))


def test_calc_value_of_campaign_all_sales_over_budget():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign'
                            '_all_sales_over_budget.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    # value = ema(sales, 28)
    kpi = df.groupby([
        "advertising_account_id",
        "portfolio_id",
        "campaign_id"],
        dropna=False
    ).apply(lambda x:
            weighted_ma(
                x.sort_values("date")["sales"],
                28
            )
            ).values
    virtual_cost = (df['yesterday_target_cost'] * df['weight']).values
    actual_cost = df['yesterday_costs'].values
    value = calculator._calc_value_of_campaign(df, daily_df).values
    assert all(value[i] == pytest.approx(
        kpi[i] * min(actual_cost[i], virtual_cost[i]) / actual_cost[i])
        for i in range(len(df)))


def test_calc_value_of_campaign_bad_purpose():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign_bad_purpose.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    with pytest.raises(NotImplementedError):
        calculator._calc_value_of_campaign(df, daily_df)


def test_calc_value_of_campaign_zero_cost():
    csv_path = os.path.join(BASE_DIR,
                            'test_calc_value_of_campaign_zero_cost.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    value = calculator._calc_value_of_campaign(df, daily_df).values
    assert all(value[i] == pytest.approx(0)
               for i in range(len(df)))


def test_grad():
    '''Does not guarantee numerical accuracy of calculator._gradient.'''
    csv_path = os.path.join(BASE_DIR,
                            'test_grad.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    df['value_of_campaign'] = calculator._calc_value_of_campaign(df, daily_df)
    result_df = calculator._gradient(df)
    assert not np.isnan(result_df["p"]).any()


def test_grad_zero_maxq():
    csv_path = os.path.join(BASE_DIR,
                            'test_grad_zero_maxq.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    df['value_of_campaign'] = calculator._calc_value_of_campaign(df, daily_df)
    result_df = calculator._gradient(df)
    assert np.allclose(result_df["p"], 1)  # since exp(0) = 1


def test_update():
    '''Does not guarantee numerical accuracy of calculator._update_weight.'''
    csv_path = os.path.join(BASE_DIR,
                            'test_grad.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    sum_weight = df.groupby(UNIT_PK_COLS)['weight'] \
        .sum().values
    assert all(sum_weight[i] == pytest.approx(1)
               for i in range(len(sum_weight)))
    df['value_of_campaign'] = calculator._calc_value_of_campaign(df, daily_df)
    df = calculator._update_weight(df)
    assert not df['weight'].isnull().any()
    sum_weight = df.groupby(UNIT_PK_COLS)['weight'] \
        .sum().values
    assert all(sum_weight[i] == pytest.approx(1)
               for i in range(len(sum_weight)))


def test_update_zero_maxq():
    csv_path = os.path.join(BASE_DIR, 'test_grad_zero_maxq.csv')
    calculator, (df, daily_df) = read_csv(csv_path)
    df['weight'] = calculator._init_weight(df, daily_df, daily_df)
    df['value_of_campaign'] = calculator._calc_value_of_campaign(df, daily_df)
    weight_before = df['weight'].values
    result_df = calculator._update_weight(df)

    assert np.allclose(result_df["weight"], weight_before)


def test_calculator_calc(today, yesterday):
    def df():
        df = pd.DataFrame(
            {
                "advertising_account_id": [779, 780, 780],
                "portfolio_id": [28, 34, 34],
                "campaign_id": [462118, 462120, 462121],
                "purpose": ['SALES', 'CONVERSION', 'CLICK'],
                'mode': [MODE_KPI, MODE_KPI, MODE_KPI],
                "optimization_costs": [100000, 50000, 10000],
                "yesterday_costs": [711.17, 1447.9, 1447.9],
                'today_target_cost': [1000] * 3,
                'today_noboost_target_cost': [1000] * 3,
                'yesterday_target_cost': [1000] * 3,
                'ideal_target_cost': [100] * 3,
                'minimum_daily_budget': [0] * 3,
                'maximum_daily_budget': [10 ** 8] * 3,
                "yesterday_daily_budget": [1000] * 3,
                "used_costs": [20000.1, 20000.1, 20000.1],
                "weight": [np.nan, np.nan, np.nan],
                'date': [today] * 3,
                "today_coefficient": [1.0] * 3,
                "yesterday_coefficient": [1.0] * 3,
                'C': [0.1] * 3,
                'unit_ex_observed_C': [0.1] * 3,
                'unit_weekly_ema_costs': [0.1] * 3,
                'campaign_weekly_ema_costs': [0.1] * 3,
                'campaign_observed_C_yesterday_in_month': [0.1] * 3,
            }
        )
        df['portfolio_id'] = df['portfolio_id'].astype("Int64")
        df['date'] = pd.to_datetime(df['date'])
        df['remaining_days'] = remaining_days(df)
        return df

    def daily_df():
        dfs = list()
        for i in range(28):
            df = pd.DataFrame(
                {
                    "advertising_account_id": [779, 780, 780],
                    "portfolio_id": [28, 34, 34],
                    "campaign_id": [462118, 462120, 462121],
                    "clicks": [0, 0, 0],
                    "conversions": [2875, 2445, 871],
                    "sales": [11393, 15785, 6758],
                    "costs": [711.17, 1447.9, 1447.9],
                }
            )
            df["date"] = yesterday - datetime.timedelta(days=i)
            dfs.append(df)
        df = pd.concat(dfs, axis=0, ignore_index=True)
        df['portfolio_id'] = df['portfolio_id'].astype("Int64")
        df['date'] = pd.to_datetime(df['date'])
        df['remaining_days'] = remaining_days(df)
        return df

    def campaign_all_actual_df():
        dfs = list()
        for i in range(28):
            df = pd.DataFrame(
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
            df["date"] = yesterday - datetime.timedelta(days=i)
            df['portfolio_id'] = df['portfolio_id'].astype("Int64")
            dfs.append(df)
        df = pd.concat(dfs, axis=0, ignore_index=True)
        return df

    _df = df()
    _daily_df = daily_df()
    _campaign_all_actual_df = campaign_all_actual_df()

    _daily_df['daily_budget'] = np.nan

    calculator = CAPCalculator(today)
    df = calculator.calc(_df, _daily_df, _campaign_all_actual_df)

    assert all(df['weight'] > 0)

    df2 = calculator.calc(_df, _daily_df, _campaign_all_actual_df)
    assert np.allclose(df['daily_budget_upper'].values,
                       df2['daily_budget_upper'].values)
    assert np.allclose(df['weight'].values,
                       df2['weight'].values)
