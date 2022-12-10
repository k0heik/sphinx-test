import datetime

import numpy as np
import pytest

from module import output_csv, prepare_df, config


def _assert_none(df, col, is_none):
    if is_none:
        assert df[col].isnull().all()
    else:
        assert not df[col].isnull().any()


@pytest.mark.parametrize("is_target_pause_none", [True, False])
@pytest.mark.parametrize("is_pid_contoller_none", [True, False])
@pytest.mark.parametrize("is_bid_optimiser_none", [True, False])
@pytest.mark.parametrize("is_cap_daily_budget_none", [True, False])
@pytest.mark.parametrize("is_cap_daily_budget_lack", [True, False])
@pytest.mark.parametrize("is_lastday_ml_applied", [True, False])
@pytest.mark.parametrize("is_lastday_ml_result_unit_zero", [True, False])
def test_output_csv_prepare_df(
    mocker,
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
    bid_optimiser_df,
    cap_daily_budget_df,
    target_pause_df,
    pid_controller_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    ad_input_json_df,
    lastday_ml_result_unit_df,
    is_target_pause_none,
    is_pid_contoller_none,
    is_bid_optimiser_none,
    is_cap_daily_budget_none,
    is_cap_daily_budget_lack,
    is_lastday_ml_applied,
    is_lastday_ml_result_unit_zero
):
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    mocker.patch("module.args.Event.today", today)
    mocker.patch("module.args.Event.advertising_account_id", 1)
    mocker.patch("module.args.Event.portfolio_id", None)

    target_ad_df, target_campaign_df, target_unit_df = prepare_df.commons(
        unit_info_df=unit_info_df,
        campaign_info_df=campaign_info_df,
        campaign_all_actual_df=campaign_all_actual_df,
        ad_info_df=ad_info_df,
        ad_target_actual_df=ad_target_actual_df,
        daily_budget_boost_coefficient_df=daily_budget_boost_coefficient_df,
    )

    target_pause_df = None if is_target_pause_none else target_pause_df
    pid_controller_df = None if is_pid_contoller_none else pid_controller_df
    bid_optimiser_df = None if is_bid_optimiser_none else bid_optimiser_df
    lastday_ml_result_unit_df = (
        lastday_ml_result_unit_df.truncate(after=-1)
        if is_lastday_ml_result_unit_zero else lastday_ml_result_unit_df
    )

    if is_cap_daily_budget_none:
        cap_daily_budget_df = None
    elif is_cap_daily_budget_lack:
        cap_daily_budget_df["campaign_id"] = -1

    (
        result_unit_df,
        result_campaign_df,
        result_ad_df,
    ) = output_csv.output_csv._prepare_df(
        target_ad_df,
        target_campaign_df,
        target_unit_df,
        is_lastday_ml_applied,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        bid_optimiser_df,
        target_pause_df,
        cap_daily_budget_df,
        pid_controller_df,
        ad_input_json_df,
        lastday_ml_result_unit_df,
    )

    assert len(result_unit_df) == 1
    assert np.all(result_unit_df["date"] == today)
    assert np.all(result_unit_df["is_lastday_ml_applied"] == is_lastday_ml_applied)

    assert len(result_campaign_df) == len(
        target_campaign_df.groupby(config.CAMPAIGN_KEY)
    )
    assert np.all(result_campaign_df["date"] == today)

    assert len(result_ad_df) == len(ad_info_df)
    assert np.all(result_ad_df["date"] == today)

    _assert_none(result_unit_df, "p", is_pid_contoller_none)
    _assert_none(result_ad_df, "bidding_price", is_bid_optimiser_none)
    _assert_none(result_campaign_df, "cap_daily_budget", is_cap_daily_budget_none or is_cap_daily_budget_lack)
    _assert_none(result_ad_df, "is_target_pause", is_target_pause_none)
    _assert_none(result_unit_df, "yesterday_target_cost", is_lastday_ml_result_unit_zero)
