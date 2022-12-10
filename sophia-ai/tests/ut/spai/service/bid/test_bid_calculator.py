import uuid
import numpy as np
import pandas as pd
import pytest
from itertools import product

from spai.optim.models import (
    KPI,
    Purpose,
)
from spai.utils.kpi.kpi import (
    MODE_BUDGET,
    MODE_KPI,
)
from spai.service.bid.preprocess import (
    get_C
)
from spai.service.bid.calculator import (
    BIDCalculator,
    get_purpose,
    get_kpi,
    estimate_ad_cpc,
    OUTPUT_DTYPES,
)


@pytest.fixture
def df():
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [1] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range("2021-04-01", f"2021-04-{n:>02}"),
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "bidding_price": [100] * n,
            "minimum_bidding_price": [2] * n,
            "maximum_bidding_price": [1000] * n,
            "impressions": [100] * n,
            "target_cost": [10000] * n,
            "base_target_cost": [10000] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
            "ctr": [1.0] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "cpc": [1.0] * n,
            "optimization_costs": [10000] * n,
            "purpose": ["SALES"] * n,
            "mode": [MODE_KPI] * n,
            "target_kpi": [np.nan] * n,
            "target_kpi_value": [np.nan] * n,
            "p": [0.01] * n,
            "q": [None] * n,
            "sum_costs": [100] * n,
            "round_up_point": [1] * n,
            "unit_ex_observed_C": [0.1] * n,
            "unit_weekly_ema_costs": [100000] * n,
            "ad_weekly_ema_costs": [0] * n,
            "ad_observed_C_yesterday_in_month": [0.1] * n,
            "C": [0.1] * n,
        }
    )


@pytest.fixture
def campaign_all_actual_df(df):
    return df[[
        "advertising_account_id",
        "portfolio_id",
        "campaign_id",
        "date",
        "impressions",
        "clicks",
        "costs",
        "conversions",
        "sales",
    ]]


@pytest.fixture
def unit_df(campaign_all_actual_df):
    return campaign_all_actual_df.drop(["campaign_id"], axis=1)


def is_calc(purpose, kpi):
    purpose = get_purpose(purpose)
    kpi = get_kpi(kpi)
    return (purpose is Purpose.CLICK and kpi is KPI.NULL) or \
        (purpose is Purpose.CLICK and kpi is KPI.CPC) or \
        (purpose is Purpose.CONVERSION and kpi is KPI.NULL) or \
        (purpose is Purpose.CONVERSION and kpi is KPI.CPA) or \
        (purpose is Purpose.SALES and kpi is KPI.NULL) or \
        (purpose is Purpose.SALES and kpi is KPI.ROAS)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("SALES", Purpose.SALES),
        ("CONVERSION", Purpose.CONVERSION),
        ("CLICK", Purpose.CLICK),
        (None, Purpose.SALES),
    ],
)
def test_get_purpose(value, expected):
    out = get_purpose(value)
    assert out is expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("NULL", KPI.NULL),
        ("ROAS", KPI.ROAS),
        ("CPA", KPI.CPA),
        ("CPC", KPI.CPC),
        (None, KPI.NULL),
    ],
)
def test_get_kpi(value, expected):
    out = get_kpi(value)
    assert out is expected


@pytest.mark.parametrize(
    "ad_clicks, ad_costs, unit_clicks, unit_costs, ad_expected, unit_expected, cpc_expected",
    [
        (10, 20, 10, 20, 6.0, 2.0, 2.0),
        (10, 20, 10, 30, 9.0, 3.0, 2.0),
        (0, 0, 10, 30, 9.0, 3.0, 0.0),
        (0, 0, 0, 0, None, None, 0.0),
    ],
)
def test_calc_bid_cpc_limit(
    df, unit_df,
    ad_clicks, ad_costs, unit_clicks, unit_costs, ad_expected, unit_expected, cpc_expected
):
    ad_df = df.copy()

    ad_df['clicks'] = ad_clicks
    ad_df['costs'] = ad_costs
    unit_df['clicks'] = unit_clicks
    unit_df['costs'] = unit_costs

    ad_result, unit_result, ad_ema_period_cpc = BIDCalculator(
        df["date"].max())._calc_bid_cpc_limit(unit_df, ad_df)

    if ad_expected:
        assert ad_result == pytest.approx(ad_expected)
        assert unit_result == pytest.approx(unit_expected)
        assert ad_ema_period_cpc == cpc_expected
    else:
        assert ad_result is ad_expected
        assert unit_expected is unit_expected
        assert ad_ema_period_cpc == cpc_expected


@pytest.mark.parametrize(
    "clicks, costs, expected",
    [
        (10, 10, 1),
        (10, 0, 0),
        (0, 0, 0),
    ],
)
def test_estimate_ad_cpc(df, clicks, costs, expected):
    df['clicks'] = clicks
    df['costs'] = costs

    result_df = estimate_ad_cpc(7, df)
    assert result_df == expected


@pytest.mark.parametrize(
    "purpose, p, clicks, expected",
    [
        (None, 0.1, 0, False),
        (None, 0.1, 1, True),
    ],
)
def test_is_by_ml(df, purpose, p, clicks, expected):
    df['purpose'] = purpose
    df['p'] = p
    df['clicks'] = clicks

    assert BIDCalculator(
        df["date"].max()
    )._is_by_ml(df) == expected


@pytest.mark.parametrize(
    "purpose, conversions, sales, expected",
    [
        ("CLICK", 0, 0, False),
        ("CLICK", 1, 1, False),
        ("CLICK", None, None, False),
        ("CONVERSION", 0, None, True),
        ("CONVERSION", 0, 0, True),
        ("CONVERSION", 1, 0, False),
        ("CONVERSION", 1, 1, False),
        ("SALES", 0, 0, True),
        ("SALES", None, 0, True),
        ("SALES", 0, 1, False),
        ("SALES", 1, 1, False),
        # 想定外の値の場合はSALES扱い
        ("", 0, 1, False),
        (f"{uuid.uuid4()}", 0, 1, False),
    ],
)
def test_is_provisional_bidding(df, purpose, conversions, sales, expected):
    df['purpose'] = purpose
    df['conversions'] = conversions
    df['sales'] = sales

    assert BIDCalculator(
        df["date"].max()
    )._is_provisional_bidding(df) == expected


def test_check_abnormal(df):
    calculator = BIDCalculator(df["date"].max())
    assert not calculator._check_abnormal(1e5)

    with pytest.raises(OverflowError):
        assert calculator._check_abnormal(1e1000)


@pytest.mark.parametrize("prev_bid", [2.0, None])
def test_get_prev_bid(df, prev_bid):
    calculator = BIDCalculator(df["date"].max())
    df.loc[-1, 'bidding_price'] = prev_bid
    result_df = calculator._get_prev_bid(df)
    if prev_bid is None:
        assert np.isnan(result_df)
    else:
        assert result_df == pytest.approx(prev_bid)


@pytest.mark.parametrize(
    "prev_bid, target_cost, base_target_cost, unit_weekly_cpc, imp, clicks, costs, expected",
    [
        (100, 1000, 500, 10, 7, 10, 300, 110),  # 週のimpが50未満 → 入札額上げる
        (100, 1000, 500, 10, 50, 10, 300, 100),  # 週のimpが50以上 → 入札額そのまま
    ]
)
def test_calc_bidding_price_by_rule(
        unit_df, df, prev_bid, target_cost, base_target_cost,
        unit_weekly_cpc, imp, clicks, costs, expected):
    df['bidding_price'] = prev_bid
    df['target_cost'] = target_cost
    df['base_target_cost'] = base_target_cost
    df['impressions'] = imp
    df['clicks'] = clicks
    df['costs'] = costs

    unit_df["clicks"] = 1
    unit_df["costs"] = unit_weekly_cpc

    calculator = BIDCalculator(df["date"].max())
    bid = calculator._calc_bidding_price_by_rule(unit_df, df)
    calculator._check_abnormal(bid)
    assert bid == pytest.approx(expected)


@pytest.mark.parametrize("kpi", ["NULL", "ROAS", "CPA", "CPC"])
@pytest.mark.parametrize("purpose", ["SALES", "CONVERSION", "CLICK"])
@pytest.mark.parametrize("is_provisional_bidding", [True, False])
@pytest.mark.parametrize("conversions", [0, 100])
def test_calc_bidding_price_by_ml(df, kpi, purpose, is_provisional_bidding, conversions):
    calculator = BIDCalculator(df["date"].max())
    df['target_cost'] = 1000
    df['base_target_cost'] = 1000
    df['target_kpi'] = kpi
    df['target_kpi_value'] = 100
    df['purpose'] = purpose
    df['conversions'] = conversions
    df['q'] = 0.1
    df['C'] = df.apply(get_C, axis=1)
    if (
        (not is_provisional_bidding and is_calc(purpose, kpi))
        or (is_provisional_bidding and (is_calc(purpose, kpi) and purpose in ["CONVERSION", "SALES"]))
        or (is_provisional_bidding and purpose == "SALES" and conversions == 0)
    ):
        (
            bid,
            origin_bid,
            unit_cpc,
            ad_ema_weekly_cpc,
            ad_value,
            sum_click_last_four_weeks,
            sum_cost_last_four_weeks,
            cpc_last_four_weeks,
        ) = calculator._calc_bidding_price_by_ml(df, df, is_provisional_bidding)

        if is_provisional_bidding:
            assert sum_click_last_four_weeks is not None
            assert sum_cost_last_four_weeks is not None
            assert cpc_last_four_weeks is not None
        else:
            assert sum_click_last_four_weeks is None
            assert sum_cost_last_four_weeks is None
            assert cpc_last_four_weeks is None
    else:
        with pytest.raises(ValueError):
            _ = calculator._calc_bidding_price_by_ml(df, df, is_provisional_bidding)


@pytest.mark.parametrize("kpi", ["NULL", "ROAS", "CPA", "CPC"])
@pytest.mark.parametrize("kpi_value", [100])
@pytest.mark.parametrize("purpose", ["SALES", "CONVERSION", "CLICK"])
@pytest.mark.parametrize("is_df_zero", [True, False])
@pytest.mark.parametrize("is_campaign_all_actual_df_zero", [True, False])
def test_calc(df, campaign_all_actual_df, kpi, kpi_value, purpose, is_df_zero, is_campaign_all_actual_df_zero):
    df['target_cost'] = 1000
    df['base_target_cost'] = 1000
    df['target_kpi'] = kpi
    df['target_kpi_value'] = kpi_value
    df['purpose'] = purpose
    df['q'] = 0.1
    df['C'] = df.apply(get_C, axis=1)

    calculator = BIDCalculator(df["date"].max())

    if is_df_zero:
        df = pd.DataFrame()

    if is_campaign_all_actual_df_zero:
        campaign_all_actual_df = pd.DataFrame()

    result_df = calculator.calc(df, campaign_all_actual_df)

    if is_df_zero or is_campaign_all_actual_df_zero:
        assert len(result_df) == 0
        assert not result_df["has_exception"].any()
    elif is_calc(purpose, kpi):
        assert len(result_df) == 1
        assert not result_df["has_exception"].any()
    else:
        expected = df['bidding_price'].values[-2]
        assert result_df['bidding_price'].values[-1] == expected
        assert result_df["has_exception"].all()

    for col in OUTPUT_DTYPES.keys():
        assert col in result_df.columns


@pytest.mark.parametrize('cpc_multiplier', [1, 2, 5])
@pytest.mark.parametrize(
    'kpi, purpose, base_target_cost',
    product(["NULL", "ROAS", "CPA", "CPC"], ["SALES", "CONVERSION", "CLICK"], [80, 120])
)
def test_calc_result(campaign_all_actual_df, df, cpc_multiplier, kpi, purpose, base_target_cost):
    df['p'] = 0.1
    df['q'] = 0.1
    df['target_cost'] = 100
    df['base_target_cost'] = base_target_cost
    df['target_kpi'] = kpi
    df['purpose'] = purpose
    df['target_kpi_value'] = 3000
    df['C'] = df.apply(get_C, axis=1)

    calculator = BIDCalculator(df["date"].max())
    if is_calc(purpose, kpi):
        result_df = calculator.calc(df, campaign_all_actual_df)
        bid = result_df['origin_bidding_price'].values[-1]

        # CPCの増加に対して入札額が減少することを確認する
        df['costs'] = df['costs'] * cpc_multiplier
        df['cpc'] = df['costs'] / df['clicks']

        result_df = calculator.calc(df, campaign_all_actual_df)
        mul_bid = result_df['origin_bidding_price'].values[-1]
        assert pytest.approx(bid / cpc_multiplier) == mul_bid
    else:
        result_df = calculator.calc(df, campaign_all_actual_df)
        expected = df['bidding_price'].values[-2]
        assert result_df['bidding_price'].values[-1] == expected


@pytest.mark.parametrize('is_by_ml', [True, False])
def test_calc_exception(df, campaign_all_actual_df, mocker, is_by_ml):
    if is_by_ml:
        mock_method_name = "spai.service.bid.calculator.BIDCalculator._calc_bidding_price_by_ml"
    else:
        mock_method_name = "spai.service.bid.calculator.BIDCalculator._calc_bidding_price_by_rule"
        df['clicks'] = 0
    bid_mock = mocker.patch(mock_method_name, side_effect=Exception("Test Error."))
    df['bidding_price'] = np.arange(0, len(df))
    expected = df['bidding_price'].values[-2]
    calculator = BIDCalculator(df["date"].max())
    result_df = calculator.calc(df, campaign_all_actual_df)
    assert result_df['bidding_price'].values[-1] == expected
    assert result_df['has_exception'].values[-1]
    bid_mock.assert_called_once()


@pytest.mark.parametrize('is_month_first_day', [True, False])
@pytest.mark.parametrize('mode, is_good_unit_performence, is_costs_excessive, is_expect_check_clip_upper', [
    (MODE_KPI, True, True, False),
    (MODE_KPI, False, True, True),
    (MODE_KPI, True, False, False),
    (MODE_KPI, False, False, True),
    (MODE_BUDGET, True, True, False),
    (MODE_BUDGET, False, True, True),
    (MODE_BUDGET, True, False, False),
    (MODE_BUDGET, False, False, False),
])
@pytest.mark.parametrize('is_high_percentage_in_cost', [True, False])
@pytest.mark.parametrize('is_bad_performance', [True, False])
@pytest.mark.parametrize('is_prev_bid_none', [True, False])
@pytest.mark.parametrize('is_kpi_purpose_is_none', [True, False])
def test_calc_clip_bad_performance(
    df,
    is_month_first_day,
    mode, is_good_unit_performence, is_costs_excessive, is_expect_check_clip_upper,
    is_high_percentage_in_cost, is_bad_performance, is_prev_bid_none, is_kpi_purpose_is_none,
):
    bid = 10
    prev_bid = bid - 1
    if is_prev_bid_none:
        prev_bid = None

    if is_month_first_day:
        df["date"] = pd.date_range(end="2022-09-01", freq="D", periods=len(df))

    df["mode"] = mode
    if is_good_unit_performence:
        df["unit_ex_observed_C"] = df["C"]
    else:
        df["unit_ex_observed_C"] = df["C"] + df["C"] / 10

    if is_kpi_purpose_is_none:
        df["C"] = None

    if is_costs_excessive:
        df["target_cost"] = df["base_target_cost"] - df["base_target_cost"] / 10
    else:
        df["target_cost"] = df["base_target_cost"]

    if is_high_percentage_in_cost:
        df["ad_weekly_ema_costs"] = df["unit_weekly_ema_costs"] / 99
    else:
        df["ad_weekly_ema_costs"] = df["unit_weekly_ema_costs"] / 100

    if is_bad_performance:
        df["ad_observed_C_yesterday_in_month"] = df["C"] + df["C"] / 10

    calculator = BIDCalculator(df["date"].max())
    result_bid = calculator._clip_bad_performance(bid, prev_bid, df)

    if (
        not is_month_first_day
        and is_expect_check_clip_upper
        and is_high_percentage_in_cost
        and is_bad_performance
        and not is_prev_bid_none
        and not is_kpi_purpose_is_none
    ):
        assert result_bid == prev_bid
    else:
        assert result_bid == bid
