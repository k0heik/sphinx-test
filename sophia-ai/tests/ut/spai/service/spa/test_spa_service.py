import os
import shutil
import pytest
import datetime
import numpy as np
import pandas as pd

from spai.service.spa.service import SPAPredictionService
from spai.service.spa.preprocess import SPAPreprocessor, agg_feature_cols, agg_query_feature_cols, get_dtypes
from spai.service.spa.estimator import SPAEstimator


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


def read_model(dir):
    def func():
        path = dir + '/model.bin'
        with open(path, mode='rb') as f:
            return f.read()

    return func


def write_model(dir):
    def func(binary):
        path = dir + '/model.bin'
        with open(path, mode='wb') as f:
            return f.write(binary)

    return func


@pytest.fixture
def df():
    dtypes, _ = get_dtypes()
    df = pd.DataFrame(index=np.arange(28), columns=agg_feature_cols)
    df[agg_feature_cols] = 100 * np.random.rand(28, len(agg_feature_cols))
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["unit_id"] = "1_1"
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "ad_type"
    df["ad_id"] = 1
    df["date"] = pd.date_range("2021-01-01", "2021-01-28")
    df["match_type"] = "match_type"
    df["account_type"] = "account_type"
    df["campaign_type"] = "campaign_type"
    df["targeting_type"] = "targeting_type"
    df["budget_type"] = "budget_type"
    df["optimization_purpose"] = 1
    df["budget"] = 100000
    dtypes = {k: v for k, v in dtypes.items() if k in df.columns}
    df = df[list(dtypes.keys())].astype(dtypes)
    df["uid"] = 1
    return df


@pytest.fixture
def df_query():
    days = 60
    base_date = datetime.date.fromisoformat("2021-12-15")
    df = pd.DataFrame(index=np.arange(days))
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "ad_type"
    df["ad_id"] = 1
    df["query"] = "query"
    df["date"] = pd.date_range(
        base_date,
        base_date + datetime.timedelta(days=(days - 1))
    )
    df[agg_query_feature_cols] = 100 * np.random.rand(
        days, len(agg_query_feature_cols))

    return df


def test_service(df, df_query):
    today = datetime.datetime.strptime("2021-01-28", '%Y-%m-%d')
    binary_dir = os.path.dirname(__file__) + '/tmp'
    os.makedirs(binary_dir, exist_ok=True)
    preprocessor = SPAPreprocessor(
        label_encoder_writer=write_label_encoder(binary_dir),
        label_encoder_reader=read_label_encoder(binary_dir)
    )
    estimator = SPAEstimator(
        model_writer=write_model(binary_dir),
        model_reader=read_model(binary_dir),
        output=None,
        is_tune=False
    )
    service = SPAPredictionService(preprocessor, estimator)

    service.train(df.copy(), df_query.copy())

    result = service.predict(df.copy(), df_query.copy(), today)

    # binファイルを削除
    shutil.rmtree(binary_dir)

    assert len(result) == 1
    assert result.reset_index().at[0, "date"] == today
    assert len(result.columns) == 8
