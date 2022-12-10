import pytest
import numpy as np

from module import pid_controller, prepare_df


@pytest.mark.parametrize("pid_data_days", [1, 10, 28])
@pytest.mark.parametrize("is_lastday_ml_applied", [True, False])
def test_pid_controller_prepare_df(
    mocker,
    common_data_today,
    common_data_yesterday,
    common_data_ad_num,
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
    lastday_ml_result_unit_df,
    ml_applied_history_df,
    pid_controller_input_df,
    pid_data_days,
    is_lastday_ml_applied,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
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
    mocker.patch("module.pid_controller._PID_DATA_DAYS", pid_data_days)

    target_ad_df, _, target_unit_df = prepare_df.commons(
        unit_info_df,
        campaign_info_df,
        campaign_all_actual_df,
        ad_info_df,
        ad_target_actual_df,
        daily_budget_boost_coefficient_df,
    )
    result_df = pid_controller._prepare_df(
        target_ad_df, target_unit_df, lastday_ml_result_unit_df, is_lastday_ml_applied, ml_applied_history_df,
        cpc_prediction_df, cvr_prediction_df, spa_prediction_df,
    )

    assert result_df["date"].max() == today
    assert set(result_df.columns) == set(pid_controller_input_df.columns)
    assert len(result_df) == (pid_data_days + 1) * common_data_ad_num

    assert all(
        [
            lastday_ml_result_unit_df["p"].values[-1] == x
            for x in result_df.loc[result_df["date"] == today, ["p"]].values
        ]
    )
    assert all(
        [
            np.isnan(x)
            for x in result_df.loc[result_df["date"] < yesterday, ["p"]].values
        ]
    )
