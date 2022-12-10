import pytest
import pandas as pd

from module import bid_optimiser


@pytest.mark.parametrize("is_today_pid_null, is_skip_pid_calc_state", [
    (True, False),
    (False, True),
    (False, False),
])
@pytest.mark.parametrize("is_valid_ads_zero", [True, False])
def test_bid_optimiser_exec(
    mocker,
    bid_optimiser_today,
    bid_optimiser_input_df,
    campaign_all_actual_df,
    is_today_pid_null,
    is_skip_pid_calc_state,
    is_valid_ads_zero,
):
    mocker.patch("module.args.Event.today", bid_optimiser_today)
    mocker.patch("module.args.Event.advertising_account_id", 1)
    mocker.patch("module.args.Event.portfolio_id", None)
    mocker.patch(
        "module.bid_optimiser._prepare_df", return_value=bid_optimiser_input_df
    )

    # dummy data
    pid_controller_df = pd.DataFrame({
        "is_skip_pid_calc_state": [is_skip_pid_calc_state],
        "valid_ads_num": [0 if is_valid_ads_zero else 1],
    })
    if is_today_pid_null:
        pid_controller_df = None

    result_df = bid_optimiser.exec(
        target_unit_df=None,
        target_ad_df=None,
        ad_input_json_df=None,
        cpc_prediction_df=None,
        cvr_prediction_df=None,
        spa_prediction_df=None,
        pid_controller_df=pid_controller_df,
        campaign_all_actual_df=campaign_all_actual_df,
    )

    if is_today_pid_null or is_skip_pid_calc_state or is_valid_ads_zero:
        assert result_df is None
    else:
        assert len(result_df) == len(
            bid_optimiser_input_df.groupby(["ad_type", "ad_id"], dropna=False)
        )
