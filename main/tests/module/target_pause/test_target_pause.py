import datetime
import pytest
import pandas as pd

from module import target_pause


@pytest.fixture
def ad_num():
    return 10


@pytest.fixture
def unit_info_df():
    return pd.DataFrame(
        {
            "advertising_account_id": [1],
            "portfolio_id": [None],
            "start": [datetime.datetime(1970, 1, 1)],
            "optimization_costs": [1],
        }
    )


@pytest.fixture
def target_ad_df(ad_num):
    datetime.datetime(2021, 1, 3)
    days = target_pause._OPERATION_DAYS_THRESHOLD + 10
    df = pd.DataFrame(
        {
            "advertising_account_id": [1] * days,
            "portfolio_id": [None] * days,
            "campaign_id": [1] * days,
            "ad_type": ["ad_type"] * days,
            "ad_id": [1] * days,
            "match_type": ["match_type"] * days,
            "date": pd.date_range(
                end=datetime.datetime(2021, 1, 2), freq="D", periods=days
            ),
            "costs": [0] * days,
            "sales": [0] * days,
        }
    )

    dfs = [df]
    for i in range(ad_num - 1):
        tmp_df = df.copy()
        tmp_df["ad_id"] = 1 + i + 1

        dfs.append(tmp_df)

    return pd.concat(dfs)


@pytest.mark.parametrize(
    "ad_type, match_type, sales_judge_days",
    [
        ("product_targeting", "", target_pause._BORDER_DAYS_30),
        ("", "exact", target_pause._BORDER_DAYS_30),
        ("", "broad", target_pause._BORDER_DAYS_60),
        ("", "phrase", target_pause._BORDER_DAYS_60),
    ],
)
@pytest.mark.parametrize("is_cost_over", [True, False])
@pytest.mark.parametrize("is_sales_zero", [True, False])
@pytest.mark.parametrize("is_date_record_enough", [True, False])
@pytest.mark.parametrize("is_min_date_enough", [True, False])
def test_target_pause(
    mocker,
    target_ad_df,
    unit_info_df,
    ad_type,
    match_type,
    sales_judge_days,
    is_cost_over,
    is_sales_zero,
    is_min_date_enough,
    is_date_record_enough,
):
    costs_judge_days = 30
    pause_test_ad_id = 3
    today = datetime.datetime(2021, 1, 1)
    yesterday = datetime.datetime(2020, 12, 31)

    budget = 10000 * costs_judge_days
    unit_info_df["optimization_costs"] = budget
    target_ad_df.loc[target_ad_df["ad_id"] == pause_test_ad_id, "ad_type"] = ad_type
    target_ad_df.loc[
        target_ad_df["ad_id"] == pause_test_ad_id, "match_type"
    ] = match_type
    target_ad_df["sales"] = 1
    target_ad_df["costs"] = (
        budget * target_pause._COST_RATIO_THRESHOLD
        - 1 / target_pause._COST_RATIO_THRESHOLD
    ) / costs_judge_days

    costs_dist_days = costs_judge_days
    if not is_date_record_enough:
        remain_days = 10
        costs_dist_days = remain_days
        target_ad_df.drop(
            index=target_ad_df[
                (target_ad_df["ad_id"] == pause_test_ad_id)
                & (
                    target_ad_df["date"]
                    < yesterday - datetime.timedelta(days=remain_days)
                )
            ].index,
            inplace=True,
        )

    if is_min_date_enough:
        current_min_date = target_ad_df[target_ad_df["ad_id"] == pause_test_ad_id][
            "date"
        ].min()
        comp_min_date = yesterday - datetime.timedelta(
            days=target_pause._OPERATION_DAYS_THRESHOLD
        )
        if comp_min_date < current_min_date:
            costs_dist_days -= 1
            target_ad_df.loc[
                (target_ad_df["ad_id"] == pause_test_ad_id)
                & (target_ad_df["date"] == current_min_date),
                "date",
            ] = comp_min_date

    if is_cost_over:
        target_ad_df.loc[
            (target_ad_df["ad_id"] == pause_test_ad_id)
            & (target_ad_df["date"] <= yesterday)
            & (
                target_ad_df["date"]
                >= yesterday - datetime.timedelta(days=costs_judge_days)
            ),
            "costs",
        ] = (
            budget * target_pause._COST_RATIO_THRESHOLD / costs_dist_days
        )

    if is_sales_zero:
        target_ad_df.loc[
            (target_ad_df["ad_id"] == pause_test_ad_id)
            & (target_ad_df["date"] <= yesterday)
            & (
                target_ad_df["date"]
                >= yesterday - datetime.timedelta(days=sales_judge_days)
            ),
            "sales",
        ] = 0

    mocker.patch("module.args.Event.today", today)
    unit_info_df["start"] = today
    result_df = target_pause.exec(unit_info_df, target_ad_df)

    if is_cost_over and is_sales_zero and (is_min_date_enough or is_date_record_enough):
        assert result_df[(result_df["ad_id"] == pause_test_ad_id)][
            "is_target_pause"
        ].all()
    else:
        assert not result_df[(result_df["ad_id"] == pause_test_ad_id)][
            "is_target_pause"
        ].any()

    assert not result_df[~(result_df["ad_id"] == pause_test_ad_id)][
        "is_target_pause"
    ].any()


@pytest.mark.parametrize(
    "today, is_check_day",
    [
        (datetime.datetime(2021, 1, 2), False),
        (datetime.datetime(2021, 1, 1), True),
    ],
)
@pytest.mark.parametrize("is_today_start", [True, False])
def test_target_pause_check_date(
    mocker, target_ad_df, unit_info_df, today, is_check_day, is_today_start
):
    mocker.patch("module.args.Event.today", today)
    if is_today_start:
        unit_info_df["start"] = today

    if not is_check_day and not is_today_start:
        assert target_pause.exec(unit_info_df, target_ad_df) is None
    else:
        assert isinstance(target_pause.exec(unit_info_df, target_ad_df), pd.DataFrame)
