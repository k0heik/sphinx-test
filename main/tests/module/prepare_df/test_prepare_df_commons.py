import pandas as pd
import pytest

from module import prepare_df


@pytest.mark.parametrize("is_latest_lack", [True, False])
def test_commons(
    mocker,
    common_data_today,
    common_data_yesterday,
    common_data_actual_days,
    common_data_ad_num,
    unit_info_df,
    campaign_info_df,
    campaign_all_actual_df,
    ad_info_df,
    ad_target_actual_df,
    daily_budget_boost_coefficient_df,
    is_latest_lack,
):
    yesterday = common_data_yesterday

    mocker.patch("module.args.Event.today", common_data_today)
    mocker.patch(
        "module.args.Event.advertising_account_id",
        unit_info_df["advertising_account_id"].values[0],
    )
    mocker.patch(
        "module.args.Event.portfolio_id", unit_info_df["portfolio_id"].values[0]
    )

    ad_target_actual_df["bidding_price"] = 100 + ad_target_actual_df["ad_id"]
    ad_info_df["bidding_price"] = 999 + ad_info_df["ad_id"]

    if is_latest_lack:
        ad_target_actual_df = ad_target_actual_df[
            ~(ad_target_actual_df["date"] == yesterday.strftime("%Y-%m-%d"))
        ]

    target_ad_df, target_campaign_df, target_unit_df = prepare_df.commons(
        unit_info_df=unit_info_df,
        campaign_info_df=campaign_info_df,
        campaign_all_actual_df=campaign_all_actual_df,
        ad_info_df=ad_info_df,
        ad_target_actual_df=ad_target_actual_df,
        daily_budget_boost_coefficient_df=daily_budget_boost_coefficient_df,
    )

    assert len(target_ad_df) == common_data_actual_days * common_data_ad_num
    assert len(target_campaign_df) == len(campaign_all_actual_df) / 2
    assert set(unit_info_df.columns) - set(target_ad_df.columns) == set()
    assert set(campaign_info_df.columns) - set(target_ad_df.columns) == set()
    assert set(campaign_all_actual_df.columns) - set(target_ad_df.columns) == set()
    assert set(ad_info_df.columns) - set(target_ad_df.columns) == set()
    assert set(ad_target_actual_df.columns) - set(target_ad_df.columns) == set()

    assert set([
        "unit_weekly_ema_costs",
    ]) - set(target_unit_df.columns) == set()
    print(target_campaign_df.columns)
    assert set([
        "campaign_weekly_ema_costs",
        "campaign_observed_C_yesterday_in_month",
    ]) - set(target_campaign_df.columns) == set()
    assert set([
        "ad_weekly_ema_costs",
        "ad_observed_C_yesterday_in_month",
    ]) - set(target_ad_df.columns) == set()

    assert set(target_ad_df["actual_param_campaign"].values) == set(
        campaign_all_actual_df["actual_param"].values
    )

    if is_latest_lack:
        pd.testing.assert_series_equal(
            target_ad_df[target_ad_df["date"] == yesterday]["bidding_price"],
            target_ad_df[target_ad_df["date"] == yesterday]["ad_id"] + 999,
            check_names=False,
        )
        assert set(target_ad_df[target_ad_df["date"] == yesterday]["actual_param"].values) == set([0])
        assert set(target_ad_df[~(target_ad_df["date"] == yesterday)]["actual_param"].values) == set(
            ad_target_actual_df[~(ad_target_actual_df["date"] == yesterday)][
                "actual_param"
            ].values
        )
    else:
        pd.testing.assert_series_equal(
            target_ad_df[target_ad_df["date"] == yesterday]["bidding_price"],
            target_ad_df[target_ad_df["date"] == yesterday]["ad_id"] + 100,
            check_names=False,
        )
        assert set(target_ad_df["actual_param"].values) == set(
            ad_target_actual_df["actual_param"].values
        )
