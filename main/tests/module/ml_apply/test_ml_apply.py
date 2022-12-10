from itertools import chain
import datetime
import math
import pandas as pd
import numpy as np
import pytest

from module import ml_apply


@pytest.fixture
def ad_num():
    return 10


@pytest.fixture
def today():
    return datetime.datetime(2022, 1, 1)


@pytest.fixture
def yesterday():
    return datetime.datetime(2021, 12, 31)


@pytest.fixture
def ad_target_actual_df(today, ad_num):
    days = 10
    return pd.DataFrame(
        {
            "date": list(pd.date_range(end=today, freq="D", periods=days)) * ad_num,
            "ad_type": ["ad_type"] * ad_num * days,
            "ad_id": chain.from_iterable([[i] * days for i in range(1, 1 + ad_num)]),
            "bidding_price": [0.0] * ad_num * days,
        }
    )


@pytest.fixture
def lastday_ml_result_ad_df(ad_num):
    return pd.DataFrame(
        {
            "ad_type": ["ad_type"] * ad_num,
            "ad_id": range(1, 1 + ad_num),
            "bidding_price": [0.0] * ad_num,
        }
    )


@pytest.mark.parametrize("is_bidding_price_match", [True, False])
def test_ml_apply(
    mocker,
    today,
    yesterday,
    ad_num,
    ad_target_actual_df,
    lastday_ml_result_ad_df,
    is_bidding_price_match,
):

    mocker.patch("module.args.Event.today", today)

    base_bidding_price = 100
    ad_target_actual_df.loc[
        ad_target_actual_df["date"] == yesterday, "bidding_price"
    ] = range(base_bidding_price, base_bidding_price + ad_num)
    if is_bidding_price_match:
        lastday_ml_result_ad_df["bidding_price"] = range(
            base_bidding_price, base_bidding_price + ad_num
        )
    else:
        lastday_ml_result_ad_df["bidding_price"] = range(
            base_bidding_price + 1, base_bidding_price + 1 + ad_num
        )

    assert (
        ml_apply.exec(ad_target_actual_df, lastday_ml_result_ad_df)
        == is_bidding_price_match
    )


def test_ml_apply_threshold(
    mocker, today, yesterday, ad_num, ad_target_actual_df, lastday_ml_result_ad_df
):

    mocker.patch("module.args.Event.today", today)

    under_threshold = math.floor(ad_num / 2)
    over_threshold = under_threshold + 1

    ad_target_actual_df.loc[
        ad_target_actual_df["date"] == yesterday, "bidding_price"
    ] = range(1, 1 + ad_num)
    lastday_ml_result_ad_df["bidding_price"] = 0.0
    lastday_ml_result_ad_df.loc[
        lastday_ml_result_ad_df["ad_id"] <= under_threshold, "bidding_price"
    ] = range(1, 1 + under_threshold)

    assert not ml_apply.exec(ad_target_actual_df, lastday_ml_result_ad_df)

    lastday_ml_result_ad_df["bidding_price"] = 0.0
    lastday_ml_result_ad_df.loc[
        lastday_ml_result_ad_df["ad_id"] <= over_threshold, "bidding_price"
    ] = range(1, 1 + over_threshold)

    assert ml_apply.exec(ad_target_actual_df, lastday_ml_result_ad_df)


def test_ml_apply_with_empty_result(mocker, today, ad_target_actual_df):
    mocker.patch("module.args.Event.today", today)
    assert not ml_apply.exec(ad_target_actual_df, pd.DataFrame())


@pytest.mark.parametrize("nan_value", [None, np.nan, pd.NA])
@pytest.mark.parametrize("is_both_nan", [True, False])
def test_ml_apply_lastday_bidding_price_null(
    mocker,
    today,
    yesterday,
    ad_target_actual_df,
    lastday_ml_result_ad_df,
    nan_value,
    is_both_nan,
):

    mocker.patch("module.args.Event.today", today)

    ad_target_actual_df.loc[
        ad_target_actual_df["date"] == yesterday, "bidding_price"
    ] = nan_value
    lastday_ml_result_ad_df["bidding_price"] = nan_value if is_both_nan else 0

    assert not ml_apply.exec(ad_target_actual_df, lastday_ml_result_ad_df)
