from module import bid_optimiser, prepare_df


def test_bid_optimiser_prepare_df(
    mocker,
    common_data_today,
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
    ad_input_json_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    pid_controller_df,
    bid_optimiser_input_df,
):
    today = common_data_today

    mocker.patch("module.args.Event.today", today)
    mocker.patch(
        "module.args.Event.advertising_account_id",
        unit_info_df["advertising_account_id"].values[0],
    )
    mocker.patch(
        "module.args.Event.portfolio_id", unit_info_df["portfolio_id"].values[0]
    )

    target_ad_df, _, target_unit_df = prepare_df.commons(
        unit_info_df,
        campaign_info_df,
        campaign_all_actual_df,
        ad_info_df,
        ad_target_actual_df,
        daily_budget_boost_coefficient_df,
    )

    result_df = bid_optimiser._prepare_df(
        target_unit_df,
        target_ad_df,
        ad_input_json_df,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        pid_controller_df,
    )

    assert result_df["date"].max() == today
    assert set(result_df.columns) == set(bid_optimiser_input_df.columns)
