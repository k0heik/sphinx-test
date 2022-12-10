import datetime
import numpy as np
import pandas as pd

import pytest

from spai.optim.models import Performance
from spai.optim.utils import (
    ctr,
    cvr,
    rpc,
    cpc,
    cpa,
    inv_roas,
    roas,
    clip,
    weighted_ma_weight,
    weighted_ma,
    weighted_cpc,
    weighted_cpc_by_df,
)


@pytest.fixture
def p():
    return Performance(
        impressions=100,
        clicks=10,
        conversions=1,
        sales=100,
        costs=100,
        bidding_price=2.0,
        cvr=0.1,
        rpc=10.0,
        cpc=10,
        date=datetime.date(2021, 4, 1),
    )


def test_ctr(p):
    p.impressions = 10
    p.clicks = 1
    assert ctr(p) == pytest.approx(0.1)
    p.impressions = 0
    assert ctr(p) == pytest.approx(0.0)


def test_cvr(p):
    p.clicks = 10
    p.conversions = 1
    assert cvr(p) == pytest.approx(0.1)
    p.conversions = 0
    assert cvr(p) == pytest.approx(0.0)


def test_rpc(p):
    p.clicks = 10
    p.sales = 1
    assert rpc(p) == pytest.approx(0.1)
    p.clicks = 0
    assert rpc(p) == pytest.approx(0.0)


def test_cpc(p):
    p.clicks = 10
    p.costs = 1
    assert cpc(p) == pytest.approx(0.1)
    p.clicks = 0
    assert cpc(p) == pytest.approx(0.0)


def test_cpa(p):
    p.conversions = 10
    p.costs = 1
    assert cpa(p) == pytest.approx(0.1)
    p.conversions = 0
    assert cpa(p) == pytest.approx(0.0)


def test_inv_roas(p):
    p.sales = 10
    p.costs = 1
    assert inv_roas(p) == pytest.approx(0.1)
    p.sales = 0
    assert inv_roas(p) == pytest.approx(0.0)


def test_roas(p):
    p.costs = 10
    p.sales = 1
    assert roas(p) == pytest.approx(0.1)
    p.costs = 0
    assert roas(p) == pytest.approx(0.0)


@pytest.mark.parametrize('x', [1, 2, 5])
@pytest.mark.parametrize('lower', [None, 2, 5])
@pytest.mark.parametrize('upper', [None, 5, 10])
def test_clip(x, lower, upper):
    result = clip(x, lower, upper)
    if lower is not None and x < lower:
        assert result == lower
    elif upper is not None and x > upper:
        assert result == upper
    else:
        assert result == x


def test_weighted_cpc():
    clicks = np.array([1, 2] + [0] * 5)
    costs = np.array([10, 14] + [0] * 5)
    cpc = weighted_cpc(7, clicks, costs)
    assert isinstance(cpc, float)
    assert cpc == pytest.approx(7.6)
    assert cpc > 0

    clicks = np.array([0] * 5 + [1, 3])
    costs = np.array([0] * 5 + [10, 13])
    cpc = weighted_cpc(7, clicks, costs)
    assert isinstance(cpc, float)
    assert cpc == pytest.approx(5.75)
    assert cpc > 0

    clicks = np.array([0] * 7)
    costs = np.array([0] * 7)
    cpc = weighted_cpc(7, clicks, costs)
    assert isinstance(cpc, float)
    assert cpc == 0.0


@pytest.mark.parametrize("window_size", [1, 7, 14, 30])
def test_weighted_ma_weight(window_size):
    weight = weighted_ma_weight(window_size)
    assert len(weight) == window_size


def test_weighted_ma():
    x = np.array(
        [
            106.0,
            131.0,
            82.0,
            87.0,
            94.0,
            90.0,
            106.0,
            79.0,
            93.0,
            97.0,
            82.0,
            85.0,
            91.0,
            82.0,
            70.0,
            63.0,
            84.0,
            78.0,
            69.0,
            66.0,
            63.0,
            89.0,
            98.0,
            71.0,
            69.0,
            83.0,
            70.0,
            78.0,
            81.0,
            70.0,
            61.0,
            63.0,
            69.0,
            62.0,
            73.0,
            60.0,
            78.0,
            75.0,
            60.0,
            56.0,
            62.0,
            59.0,
            52.0,
            49.0,
            54.0,
            45.0,
            61.0,
            49.0,
            48.0,
            46.0,
            55.0,
            56.0,
            43.0,
            45.0,
            49.0,
            40.0,
            41.0,
            46.0,
            34.0,
            51.0,
            36.0,
            28.0,
            28.0,
            34.0,
            25.0,
            46.0,
            36.0,
            34.0,
            35.0,
            36.0,
            26.0,
            28.0,
            35.0,
            29.0,
            23.0,
            24.0,
            21.0,
            23.0,
            28.0,
            19.0,
            22.0,
            19.0,
            21.0,
            19.0,
            19.0,
            14.0,
            7.0,
            12.0,
            11.0,
            11.0,
            5.0,
            8.0,
            6.0,
            6.0,
            6.0,
            6.0,
            2.0,
            4.0,
            3.0,
            0.0,
        ]
    )
    y = weighted_ma(x, 28)
    gold = np.array(
        [
            106.0,
            118.5,
            106.3375,
            101.13333333333334,
            99.51937984496124,
            97.77631578947367,
            98.98850574712642,
            96.24102564102564,
            95.73023255813953,
            95.83760683760683,
            94.33333333333334,
            93.31598513011153,
            93.02456140350877,
            91.97999999999998,
            90.00318471337579,
            87.63608562691132,
            87.16814159292036,
            86.33714285714285,
            84.8861111111111,
            83.30894308943088,
            81.62599469496021,
            81.87760416666666,
            82.79230769230769,
            81.82025316455696,
            80.69423558897242,
            80.55223880597015,
            79.57178217821782,
            79.12345679012346,
            78.89876543209877,
            78.00987654320987,
            76.64444444444445,
            75.44197530864197,
            74.70370370370371,
            73.5753086419753,
            73.2320987654321,
            72.13086419753087,
            72.24444444444444,
            72.23950617283951,
            71.28148148148148,
            70.07407407407408,
            69.32839506172839,
            68.46913580246913,
            67.19259259259259,
            65.7432098765432,
            64.65432098765432,
            63.05185185185185,
            62.57530864197531,
            61.35802469135802,
            60.08641975308642,
            58.71604938271606,
            58.04691358024691,
            57.57283950617283,
            56.2716049382716,
            55.1358024691358,
            54.365432098765424,
            53.05679012345679,
            51.88641975308642,
            51.15061728395061,
            49.68641975308642,
            49.392592592592585,
            48.17037037037037,
            46.459259259259255,
            44.81234567901234,
            43.67654320987654,
            42.019753086419755,
            41.87160493827161,
            41.18024691358025,
            40.39012345679012,
            39.71604938271605,
            39.17777777777778,
            38.032098765432096,
            37.059259259259264,
            36.60987654320987,
            35.82469135802469,
            34.66419753086419,
            33.64938271604938,
            32.49876543209876,
            31.540740740740738,
            30.977777777777774,
            29.893827160493824,
            29.079012345679015,
            28.123456790123456,
            27.358024691358025,
            26.533333333333335,
            25.755555555555553,
            24.698765432098767,
            23.24197530864198,
            22.167901234567903,
            21.135802469135808,
            20.16296296296296,
            18.832098765432097,
            17.74320987654321,
            16.59259259259259,
            15.483950617283949,
            14.474074074074075,
            13.538271604938268,
            12.404938271604937,
            11.476543209876542,
            10.56543209876543,
            9.508641975308642,
        ]
    )
    np.testing.assert_almost_equal(y, gold)


@pytest.mark.parametrize("is_denom_zero", [True, False])
def test_weighted_cpc_by_df(is_denom_zero):
    n = 20
    df = pd.DataFrame({
        "id1": [1] * n,
        "id2": [np.nan] * n,
        "costs": [100] * n,
        "clicks": [100] * n,
        "date": pd.date_range("2021-04-01", freq="D", periods=n),
    })

    df.loc[12, "costs"] = 1
    df.loc[19, "clicks"] = 153

    if is_denom_zero:
        df["clicks"] = 0

    tmp = df.copy()
    tmp["id1"] = df["id1"].max() + 1
    tmp["id2"] = None

    df = pd.concat([df, tmp])

    for _, daily_actual_df in df.groupby(["id1", "id2"], dropna=False):
        result = weighted_cpc_by_df(
            daily_actual_df, daily_actual_df["date"].max(), 14)

        if is_denom_zero:
            np.testing.assert_almost_equal(result, 0.0, decimal=6)
        else:
            np.testing.assert_almost_equal(result, 0.875372, decimal=6)
