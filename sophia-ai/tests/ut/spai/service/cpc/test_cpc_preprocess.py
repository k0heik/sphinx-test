import os
import shutil
import numpy as np
import pandas as pd
import pytest

from spai.service.cpc.preprocess import CPCPreprocessor, agg_feature_cols
from spai.service.cpc.config import FEATURE_COLUMNS


def read_label_encoder(dir):
    def func(col):
        path = dir + f'/{col}.bin'
        with open(path, mode='rb') as f:
            return f.read()

    return func


def write_label_encoder(dir):
    def func(binary, col):
        path = dir + f'/{col}.bin'
        with open(path, mode='wb') as f:
            return f.write(binary)

    return func


@pytest.fixture
def df():
    df = pd.DataFrame(index=np.arange(7), columns=agg_feature_cols)
    df['unit_id'] = "1_1"
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "keyword"
    df["ad_id"] = 1
    df["date"] = pd.date_range("2021-01-01", "2021-01-07")
    df["bidding_price"] = 100
    df[agg_feature_cols] = np.random.randn(7, len(agg_feature_cols))
    df["match_type"] = "match_type"
    df["campaign_type"] = "campaign_type"
    df["targeting_type"] = "targeting_type"
    df["budget"] = 100
    df["budget_type"] = "budget_type"
    df["account_type"] = "account_type"
    df["optimization_purpose"] = 0

    df.loc[0, "clicks"] = 0
    df.loc[1, "impressions"] = 0
    df["uid"] = 1
    return df


@pytest.fixture(params=[
    (True,),
    (False,),
])
def df_placement(request):
    is_data_zero = request.param[0]
    T = 7
    dfs = []
    for predicate in ["placementProductPage", "placementTop"]:
        df = pd.DataFrame(index=np.arange(T))
        df["campaign_id"] = 1
        df["clicks"] = 1
        df["conversions"] = 1
        df["costs"] = 1
        df["impressions"] = 1
        df["predicate"] = [predicate] * T
        df["date"] = pd.date_range("2021-01-01", periods=T, freq="D")

        dfs.append(df)

    if is_data_zero:
        return pd.DataFrame(columns=dfs[0].columns)
    else:
        return pd.concat(dfs)


@pytest.mark.parametrize("is_use_dask", [True, False])
def test_preprocess(df, df_placement, is_use_dask):
    label_dir = os.path.dirname(__file__) + '/tmp'
    os.makedirs(label_dir, exist_ok=True)
    preprocessor = CPCPreprocessor(
        label_encoder_writer=write_label_encoder(label_dir),
        label_encoder_reader=read_label_encoder(label_dir),
        is_use_dask=is_use_dask,
    )

    result_df = preprocessor.preprocess(
        df.copy(), df_placement=df_placement.copy(), is_train=True)

    # weightがclicksになっているかの確認
    np.testing.assert_array_equal(
        result_df["weight"].values,
        df["clicks"].values
    )

    for col in FEATURE_COLUMNS:
        assert col in result_df.columns

    result_df = preprocessor.preprocess(df.copy(), df_placement=df_placement.copy(), is_train=False)

    assert len(result_df) == len(df)

    for col in FEATURE_COLUMNS:
        assert col in result_df.columns

    # binファイルを削除
    shutil.rmtree(label_dir)
