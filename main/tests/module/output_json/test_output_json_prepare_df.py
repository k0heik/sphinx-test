import pytest

from module import output_json, config


@pytest.mark.parametrize("is_target_pause_none", [True, False])
@pytest.mark.parametrize("is_target_pause", [True, False])
@pytest.mark.parametrize("is_bid_none", [True, False])
@pytest.mark.parametrize("is_cap_none", [True, False])
@pytest.mark.parametrize("is_cap_lack", [True, False])
def test_output_json_prepare_df(
    mocker,
    common_data_today,
    common_data_yesterday,
    ad_input_json_df,
    bid_optimiser_df,
    cap_daily_budget_df,
    target_pause_df,
    output_json_input_df,
    is_target_pause_none,
    is_target_pause,
    is_bid_none,
    is_cap_none,
    is_cap_lack,
):
    today = common_data_today
    bidding_price = 100
    daily_budget = 200
    last_bidding_price = 1
    last_daily_budget = 2

    mocker.patch("module.args.Event.today", today)
    mocker.patch("module.args.Event.advertising_account_id", 1)
    mocker.patch("module.args.Event.portfolio_id", None)

    if is_target_pause:
        target_pause_df["is_target_pause"] = is_target_pause

    if is_target_pause_none:
        target_pause_df = None

    ad_input_json_df.loc[
        ad_input_json_df["date"] == common_data_yesterday, "bidding_price"
    ] = last_bidding_price
    ad_input_json_df.loc[
        ad_input_json_df["date"] == common_data_yesterday, "daily_budget"
    ] = last_daily_budget
    bid_optimiser_df["bidding_price"] = bidding_price
    cap_daily_budget_df["daily_budget_upper"] = daily_budget

    if is_bid_none:
        bid_optimiser_df = None

    if is_cap_none:
        cap_daily_budget_df = None
    elif is_cap_lack:
        cap_daily_budget_df["campaign_id"] = -1

    result_df = output_json.output_json._prepare_df(
        ad_input_json_df, bid_optimiser_df, cap_daily_budget_df, target_pause_df
    )

    assert set(result_df.columns) == set(output_json_input_df.columns)
    assert len(result_df) == len(ad_input_json_df.groupby(config.AD_KEY).last())
    assert set(result_df["last_bidding_price"]) == set([last_bidding_price])
    assert set(result_df["last_daily_budget"]) == set([last_daily_budget])
    assert set(result_df["is_paused"]) == set(
        [is_target_pause and not is_target_pause_none]
    )

    if is_bid_none:
        assert set(result_df["bidding_price"]) == set([last_bidding_price])
    else:
        assert set(result_df["bidding_price"]) == set([bidding_price])

    if is_cap_none or is_cap_lack:
        assert set(result_df["daily_budget"]) == set([last_daily_budget])
    else:
        assert set(result_df["daily_budget"]) == set([daily_budget])
