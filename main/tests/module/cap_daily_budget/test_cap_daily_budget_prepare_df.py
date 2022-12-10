import random

import pandas as pd
import numpy as np
import pytest

from module import cap_daily_budget, prepare_df


@pytest.fixture
def lastday_ml_result_campaign_df(campaign_info_df, campaign_all_actual_df):
    dates = campaign_all_actual_df.groupby("date").last().reset_index()["date"].values
    days = 1
    return pd.DataFrame(
        {
            "campaign_id": [campaign_info_df["campaign_id"].max()] * days,
            "date": dates[-1],
            "weight": [random.randint(1, 10**5) for _ in range(days)],
        }
    )


@pytest.mark.parametrize("cap_data_days", [1, 10, 28])
@pytest.mark.parametrize("is_lastday_ml_result_unit_df_zero", [True, False])
def test_cap_daily_budget_prepare_df(
    mocker,
    common_data_today,
    common_data_yesterday,
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
    ad_input_json_df,
    lastday_ml_result_campaign_df,
    cap_daily_budget_input_df,
    cap_daily_budget_input_daily_df,
    lastday_ml_result_unit_df,
    cap_data_days,
    is_lastday_ml_result_unit_df_zero,
):
    today = common_data_today
    yesterday = common_data_yesterday

    mocker.patch("module.args.Event.today", today)
    mocker.patch(
        "module.args.Event.advertising_account_id",
        unit_info_df["advertising_account_id"].values[0],
    )
    mocker.patch(
        "module.args.Event.portfolio_id", unit_info_df["portfolio_id"].values[0]
    )
    mocker.patch("module.cap_daily_budget._CAP_DATA_DAYS", cap_data_days)

    _, target_campaign_df, target_unit_df = prepare_df.commons(
        unit_info_df,
        campaign_info_df,
        campaign_all_actual_df,
        ad_info_df,
        ad_target_actual_df,
        daily_budget_boost_coefficient_df,
    )

    if is_lastday_ml_result_unit_df_zero:
        lastday_ml_result_unit_df = pd.DataFrame()

    result_df, result_daily_df = cap_daily_budget._prepare_df(
        ad_input_json_df, lastday_ml_result_campaign_df, lastday_ml_result_unit_df,
        target_campaign_df, target_unit_df, campaign_info_df,
        daily_budget_boost_coefficient_df,
    )

    assert (result_df["date"] == today).all()
    assert set(result_df.columns) == set(cap_daily_budget_input_df.columns)
    assert len(result_df) == len(ad_input_json_df.groupby("campaign_id"))
    assert result_daily_df["date"].max() == yesterday
    assert set(result_daily_df.columns) == set(cap_daily_budget_input_daily_df.columns)
    assert len(result_daily_df) == cap_data_days

    assert all(
        [
            lastday_ml_result_campaign_df["weight"].values[-1] == x
            for x in result_daily_df.loc[result_daily_df["date"] == yesterday, "weight"].values
        ]
    )
    assert all(
        [
            np.isnan(x)
            for x in result_daily_df.loc[result_daily_df["date"] < yesterday, "weight"].values
        ]
    )

    if is_lastday_ml_result_unit_df_zero:
        assert result_df["yesterday_target_cost"].isnull().all()
    else:
        assert not result_df["yesterday_target_cost"].isnull().any()
