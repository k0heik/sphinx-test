import datetime
import pandas as pd
import pytest

from module import prepare_df


@pytest.mark.parametrize("is_month_first", [True, False])
@pytest.mark.parametrize("is_denom_zero", [True, False])
@pytest.mark.parametrize("target_kpi", ["ROAS", "CPC", "CPA"])
@pytest.mark.parametrize("costs_value", [2, 0])
def test_prepare_df_unit_ex_observed_C(ad_target_actual_df, is_month_first, is_denom_zero, target_kpi, costs_value):
    denom_value = 0 if is_denom_zero else 2
    ad_target_actual_df["clicks"] = denom_value
    ad_target_actual_df["conversions"] = denom_value
    ad_target_actual_df["sales"] = denom_value

    ad_target_actual_df["costs"] = costs_value

    today = ad_target_actual_df["date"].max() + datetime.timedelta(days=1)
    if is_month_first:
        today = today.replace(day=1)

    ad_num = len(ad_target_actual_df.groupby(["ad_type", "ad_id"]))
    month_days = ad_target_actual_df["date"].max().day

    ret = prepare_df._unit_ex_observed_C(target_kpi, today, ad_target_actual_df)

    if is_month_first:
        assert ret is None
    elif is_denom_zero:
        assert ret == costs_value * ad_num * month_days
    else:
        assert ret == costs_value / denom_value


@pytest.mark.parametrize("target_kpi", ["ROAS", "CPC", "CPA"])
def test_prepare_df_unit_ex_observed_C__monthly_performance_zero(ad_target_actual_df, target_kpi):
    today = datetime.datetime(2022, 10, 4)
    ad_target_actual_df["clicks"] = 0
    ad_target_actual_df["conversions"] = 0
    ad_target_actual_df["sales"] = 0
    costs_value = 2
    ad_target_actual_df["costs"] = costs_value

    ad_target_actual_df["date"] = list(pd.date_range(end=today, freq="D", periods=len(ad_target_actual_df) / 2)) * 2
    ad_target_actual_df.loc[ad_target_actual_df["date"].dt.month != today.month, "clicks"] = costs_value
    ad_target_actual_df.loc[ad_target_actual_df["date"].dt.month != today.month, "conversions"] = costs_value
    ad_target_actual_df.loc[ad_target_actual_df["date"].dt.month != today.month, "sales"] = costs_value

    ad_num = len(ad_target_actual_df.groupby(["ad_type", "ad_id"]))
    month_days = ad_target_actual_df["date"].max().day

    ret = prepare_df._unit_ex_observed_C(target_kpi, today, ad_target_actual_df)

    if target_kpi == "ROAS":
        assert ret < costs_value * ad_num * (28 - month_days)
        assert ret > 0
    else:
        assert ret == costs_value * ad_num * month_days
