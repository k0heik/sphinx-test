import uuid
import random
import numpy as np
import pandas as pd
import pytest

from spai.utils.kpi import (
    agg_feats,
    calc_kpis,
    calc_lag,
    calc_mean,
    calc_placement_kpi,
    merge_agg_feats,
    merge_cutoff_feats,
    merge_feats,
    safe_div,
    ewm,
    calc_unit_weekly_cpc_for_cap,
)

agg_feature_cols = ['impressions', 'clicks',
                    'costs', 'conversions', 'sales']

FEATURE_COLUMNS = [
    'campaign_type',
    'ad_id_ewm7_cvr',
    'ad_id_weekly_clicks',
    'ad_id_weekly_costs',
    'ad_id_weekly_conversions',
    'ad_id_weekly_sales',
    'ad_id_weekly_ctr',
    'ad_id_weekly_cvr',
    'ad_id_monthly_conversions',
    'ad_id_monthly_sales',
    'ad_id_monthly_ctr',
    'ad_id_monthly_rpc',
    'campaign_id_weekly_cvr',
    'campaign_id_monthly_cvr',
    'campaign_id_monthly_rpc',
    'unit_id_ewm7_rpc',
    'unit_id_weekly_cvr',
    'diff_campaign_id_monthly_ctr',
    'diff_unit_id_ewm7_ctr',
    'diff_unit_id_weekly_ctr',
    'ad_id_weekly_sum_query_conversions',
    'ad_id_monthly_sum_query_conversions',
]


@pytest.fixture
def df():
    T = 7
    df = pd.DataFrame(index=np.arange(T), columns=agg_feature_cols)
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["unit_id"] = "1_1"
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "ad_type"
    df["ad_id"] = 1
    df["date"] = pd.date_range("2021-01-01", periods=T, freq='D')
    df["bidding_price"] = 100
    df[agg_feature_cols] = np.random.randn(7, len(agg_feature_cols))
    df["match_type"] = "match_type"
    df["campaign_type"] = "campaign_type"
    df["targeting_type"] = "targeting_type"
    df["budget"] = 100
    df["budget_type"] = "budget_type"
    df["account_type"] = "account_type"
    df["optimization_purpose"] = 0

    df.loc[0, "clicks"] = 0
    df.loc[1, "impressions"] = 0
    df["uid"] = 1
    return df


@pytest.fixture
def placement_df():
    T = 7
    df = pd.DataFrame(index=np.arange(T))
    df["campaign_id"] = 1
    df["clicks"] = 1
    df["conversions"] = 1
    df["costs"] = 1
    df["impressions"] = 1
    df["predicate"] = None
    df["date"] = pd.date_range("2021-01-01", periods=T, freq="D")

    return df


@pytest.mark.parametrize("freq", ["7D", "28D"])
@pytest.mark.parametrize("agg_name", [f"{uuid.uuid4()}"])
@pytest.mark.parametrize("key_column", ["ad_id", "campaign_id", "unit_id"])
def test_agg_feats_rolling(df, freq, agg_name, key_column):
    method = "rolling"
    result = agg_feats(df, freq, method, agg_name, key_column, FEATURE_COLUMNS)
    for i in range(len(df)):
        np.testing.assert_allclose(
            result[agg_feature_cols].values[i],
            df[agg_feature_cols].iloc[: i + 1].mean(axis=0),
        )


@pytest.mark.parametrize("freq", ["7D", "28D"])
@pytest.mark.parametrize("method", ["rolling"])
@pytest.mark.parametrize("agg_name", ["weekly", "monthly", "ema"])
@pytest.mark.parametrize("key_column", ["ad_id", "campaign_id", "unit_id"])
def test_merge_feats(df, freq, method, agg_name, key_column):
    feats = agg_feats(df, freq, method, agg_name, key_column, FEATURE_COLUMNS)
    result = merge_feats(df, feats, agg_name, key_column)
    merge_feature_cols = [f"{key_column}_{agg_name}_{col}" for col in agg_feature_cols]
    for i in range(1, len(df)):
        # Not including today
        np.testing.assert_allclose(
            result[merge_feature_cols].values[i],
            df[agg_feature_cols].iloc[:i].mean(axis=0),
        )


def test_merge_cutoff_feats(df):
    merge_cutoff_feats(df)


@pytest.mark.parametrize("freq", ["7D", "28D"])
@pytest.mark.parametrize("method", ["rolling", "ema"])
@pytest.mark.parametrize("prefix", ["weekly", "monthly", "ema"])
@pytest.mark.parametrize("key_column", ["ad_id", "campaign_id", "unit_id"])
def test_merge_agg_feats(df, freq, method, prefix, key_column):
    merge_agg_feats(df, freq, method, prefix, key_column, FEATURE_COLUMNS)


def test_safe_div():
    x = np.random.randn(5)
    y = np.random.randn(5)
    z = x / y
    y[0] = 0
    z[0] = 0
    np.testing.assert_allclose(safe_div(x, y), z)


def test_calc_kpis(df):
    kpi_cols = ["ctr", "cvr", "rpc", "spa"]
    map_dict = {
        "ctr": ("clicks", "impressions"),
        "cvr": ("conversions", "clicks"),
        "rpc": ("sales", "clicks"),
        "spa": ("sales", "conversions"),
    }
    a = calc_kpis(df)
    for col in kpi_cols:
        mask = a[map_dict[col][1]] == 0
        a['expected'] = a[map_dict[col][0]] / a[map_dict[col][1]]
        a.loc[mask, 'expected'] = 0
        np.testing.assert_allclose(a[col].values, a['expected'].values)


@pytest.mark.parametrize("key_columns", [
    ["unit_id"],
    ["campaign_id"],
    ["ad_type", "ad_id"],
])
@pytest.mark.parametrize("days", [1, 3, 7, 14])
@pytest.mark.parametrize("column", ["bidding_price", "impressions"])
@pytest.mark.parametrize("col_name_format", [f"{uuid.uuid4()}_{{day}}day"])
def test_calc_lag(df, key_columns, column, days, col_name_format):
    test_key = 52

    tmp_dfs = []
    for id in range(test_key - 5, test_key + 5):
        tmp_df = df.copy()
        tmp_df["advertising_account_id"] = id
        tmp_df["portfolio_id"] = None
        tmp_df["campaign_id"] = id
        tmp_df["ad_id"] = id
        tmp_df["unit_id"] = f"{id}_{id}"
        tmp_dfs.append(tmp_df)

    df = pd.concat(tmp_dfs + [df])

    ret_df = calc_lag(
        df, column,
        days=days,
        key_columns=key_columns,
        col_name_format=col_name_format)

    for _day in range(days):
        assert col_name_format.format(day=_day+1) in ret_df.columns

    assert_key = test_key if key_columns[-1] != "unit_id" else f"{test_key}_{test_key}"

    assert np.isnan(ret_df[
            (ret_df[key_columns[-1]] == assert_key)
            & (ret_df["date"] == "2021-01-01")
        ].reset_index().at[0, col_name_format.format(day=1)])
    assert ret_df[
            (ret_df[key_columns[-1]] == assert_key) & (ret_df["date"] == "2021-01-02")
        ].reset_index().at[0, col_name_format.format(day=1)] == \
        df[
            (df[key_columns[-1]] == assert_key) & (df["date"] == "2021-01-01")
        ].reset_index().at[0, column]


@pytest.mark.parametrize("key_columns", [
    ["unit_id"],
    ["campaign_id"],
    ["ad_type", "ad_id"],
])
@pytest.mark.parametrize("columns", [["bidding_price", "cpc"]])
@pytest.mark.parametrize("prefix", [f"{uuid.uuid4()}"])
def test_calc_mean(df, columns, key_columns, prefix):
    df[columns] = 100
    test_key = 52
    test_value = pd.Series([random.randint(0, 10) for _ in range(len(df))])
    expected_df = test_value

    tmp_dfs = []
    for id in range(test_key - 5, test_key + 5):
        tmp_df = df.copy()
        key_column = key_columns[-1]
        if key_column == "unit_id":
            tmp_df[key_column] = f"{id}_{id}"
        else:
            tmp_df[key_column] = id
        tmp_df["portfolio_id"] = None

        if id == test_key:
            for column in columns:
                tmp_df[column] = pd.Series(test_value)

        tmp_dfs.append(tmp_df)

    df = pd.concat(tmp_dfs + [df])

    ret_df = calc_mean(
        df, columns,
        key_columns=key_columns,
        prefix=prefix,
    )

    assert_key = test_key if key_columns[-1] != "unit_id" else f"{test_key}_{test_key}"

    for column in columns:
        assert all(
            ret_df[ret_df[key_columns[-1]] == assert_key].reset_index()[f"{prefix}_{column}"]
            == expected_df.astype(float)
        )


@pytest.mark.parametrize("predicates", [
    ["placementProductPage", "placementTop", "other", None],
    reversed(["placementProductPage", "placementTop", "other", None]),
    ["other", None],
    ["placementTop", "other", None],
    ["placementProductPage", "other", None],
])
@pytest.mark.parametrize("is_data_zero", [True, False])
def test_calc_placement_kpi(df, placement_df, predicates, is_data_zero):
    def _cpc_mag(c):
        return 1 + c

    tmp_dfs = []
    tmp_placement_dfs = []
    no_data_id = 1
    test_id = 100

    tmp_df = df.copy()
    tmp_df["campaign_id"] = test_id
    tmp_dfs.append(tmp_df)

    for i, predicate in enumerate(predicates):
        tmp_placement_df = placement_df.copy()

        tmp_placement_df["campaign_id"] = test_id
        tmp_placement_df["predicate"] = predicate
        tmp_placement_df["clicks"] = random.randint(1, 1000)
        tmp_placement_df["costs"] = tmp_placement_df["clicks"] * _cpc_mag(i)

        tmp_placement_dfs.append(tmp_placement_df)

    dummy_placement_df = df.copy()
    dummy_placement_df["campaign_id"] = 999
    tmp_placement_dfs.append(dummy_placement_df)

    df = pd.concat(tmp_dfs + [df])

    placement_df = pd.concat(tmp_placement_dfs + [placement_df])

    if is_data_zero:
        placement_df = pd.DataFrame(columns=placement_df.columns)

    ret_df = calc_placement_kpi(df, placement_df)

    for i, predicate in enumerate(predicates):
        if predicate in ["placementProductPage", "placementTop"]:
            column = f"{predicate}_cpc"
            assert_val = _cpc_mag(i) if not is_data_zero else np.nan

            pd.testing.assert_series_equal(
                ret_df[
                    ret_df["campaign_id"] == test_id
                ].reset_index()[column],
                pd.Series([float(assert_val)] * 7, name=column)
            )
            assert all(
                ret_df[
                    ret_df["campaign_id"] == no_data_id
                ][column].isnull())
        else:
            assert f"{predicate}_cpc" not in ret_df.columns


@pytest.mark.parametrize("date_col", ["date", "col_name"])
def test_ewm(df, date_col):
    n = 3
    dates = 5
    dfs = []
    for i in range(1, n + 1):
        date = pd.DatetimeIndex(['2022-01-01', '2022-01-03', '2022-01-10', '2022-01-15', '2022-01-17'])
        dfs.append(pd.DataFrame({
            date_col: date,
            'data_1': [0, 1, 2, np.nan, 4],
            'data_2': [0, 1, 2, np.nan, 4],
            'key_col': [i] * dates,
        }))

    df = pd.concat(dfs).reset_index()
    answer = [0.000000, 0.585786, 1.523889, 1.523889, 3.233686] * n

    halflife = '4D'
    min_periods = 1
    agg_query_feature_cols = ['data_1', 'data_2']
    res = ewm(
        df.groupby(
            'key_col',
            dropna=False,
            group_keys=False
        ),
        halflife,
        min_periods,
        agg_query_feature_cols,
        date_col=date_col
    )

    for col in ["data_1", "data_2"]:
        pd.testing.assert_series_equal(pd.Series(answer, name=col), res[col])


def test_calc_unit_weekly_cpc_for_cap(df):
    today = df['date'].max()
    df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())
    df["costs"] = 1
    df["clicks"] = 1
    out = calc_unit_weekly_cpc_for_cap(df, today)

    assert out == pytest.approx(1)
