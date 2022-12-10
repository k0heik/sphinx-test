import numpy as np
import pandas as pd
import pytest

from spai.service.bid.preprocess import (
    BIDPreprocessor,
    get_C,
)


@pytest.fixture
def df():
    n = 6
    return pd.DataFrame(
        {
            "advertising_account_id": [1] * n,
            "portfolio_id": [None] * n,
            "campaign_id": [1] * n,
            "ad_type": ["keyword"] * n,
            "ad_id": [1] * n,
            "date": pd.date_range("2021-04-01", f"2021-04-{n:>02}"),
            "is_enabled_bidding_auto_adjustment": [True] * n,
            "bidding_price": [100] * n,
            "minimum_bidding_price": [2] * n,
            "maximum_bidding_price": [1000] * n,
            "impressions": [100] * n,
            "clicks": [100] * n,
            "costs": [100] * n,
            "conversions": [100] * n,
            "sales": [100] * n,
            "ctr": [1.0] * n,
            "cvr": [1.0] * n,
            "rpc": [1.0] * n,
            "optimization_costs": [10000] * n,
            "purpose": ["SALES"] * n,
            "target_kpi": [np.nan] * n,
            "target_kpi_value": [np.nan] * n,
            "p": [0.01] * n,
            "q": [None] * n,
            "sum_costs": [100] * n,
            "round_up_point": [1] * n,
        }
    )


def test_preprocess(df):
    preprocessor = BIDPreprocessor()
    out = preprocessor.transform(df)
    assert len(out) == len(df)


@pytest.mark.parametrize(
    "target_kpi, target_kpi_value, expected",
    [
        ("ROAS", 1000, 1 / 1000),
        ("ROAS", None, None),
        ("CPA", 500, 500),
        ("CPC", 50, 50),
    ],
)
def test_get_C(df, target_kpi, target_kpi_value, expected):
    df['target_kpi'] = target_kpi
    df['target_kpi_value'] = target_kpi_value
    df['C'] = df.apply(get_C, axis=1)
    for c in df['C'].values:
        if expected is not None:
            assert c == pytest.approx(expected)
        else:
            assert c is expected


def test_fillna(df):
    preprocessor = BIDPreprocessor()
    df["impressions"] = None
    df["clicks"] = None
    df["costs"] = None
    df["conversions"] = None
    df["sales"] = None
    df["portfolio_id"] = None
    df["target_kpi_value"] = None
    df = preprocessor._fillna(df)
    assert df["impressions"].apply(lambda x: x == 0).all()
    assert df["clicks"].apply(lambda x: x == 0).all()
    assert df["costs"].apply(lambda x: x == 0).all()
    assert df["conversions"].apply(lambda x: x == 0).all()
    assert df["sales"].apply(lambda x: x == 0).all()
    assert df["portfolio_id"].apply(lambda x: x == -1).all()
    assert df["target_kpi_value"].apply(lambda x: x is None).all()
