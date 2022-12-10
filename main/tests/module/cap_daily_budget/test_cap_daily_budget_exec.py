from module import cap_daily_budget, prepare_df


def test_cap_daily_budget_exec(
    mocker,
    cap_daily_budget_today,
    cap_daily_budget_input_df,
    cap_daily_budget_input_daily_df,
    campaign_all_actual_df,
):
    mocker.patch("module.args.Event.today", cap_daily_budget_today)
    mocker.patch(
        "module.args.Event.advertising_account_id",
        cap_daily_budget_input_df["advertising_account_id"].values[0],
    )
    mocker.patch(
        "module.args.Event.portfolio_id",
        cap_daily_budget_input_df["portfolio_id"].values[0],
    )
    mocker.patch(
        "module.cap_daily_budget._prepare_df", return_value=(
            cap_daily_budget_input_df, cap_daily_budget_input_daily_df)
    )

    cap_daily_budget.exec(
        None, None, None, None, None, None,
        prepare_df.add_unit_key(campaign_all_actual_df),
        None,
    )
