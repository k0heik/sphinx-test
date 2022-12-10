import os
import shutil
import numpy as np
import pandas as pd
import pytest
import datetime

from spai.service.cvr.preprocess import (
    CVRPreprocessor,
    agg_feature_cols,
    agg_query_feature_cols,
)
from spai.service.cvr.estimator import CVREstimator
from spai.service.cvr.config import OUTPUT_COLUMNS
from spai.ai.preprocess import LabelEncoder


def read_label_encoder(dir):
    def func(col):
        path = dir + f'/{col}.bin'
        with open(path, mode='rb') as f:
            return LabelEncoder.from_bytes(f.read())

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
    df = pd.DataFrame(index=np.arange(7), columns=agg_feature_cols)
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["unit_id"] = "1_1"
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "ad_type"
    df["ad_id"] = 1
    df["date"] = pd.date_range("2021-01-01", "2021-01-07")
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


@pytest.fixture
def df_query():
    df = pd.DataFrame(index=np.arange(7), columns=agg_query_feature_cols)
    df['unit_id'] = "1_1"
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "keyword"
    df["ad_id"] = 1
    df["query"] = "query"
    df["date"] = pd.date_range("2021-01-01", "2021-01-07")
    df[agg_query_feature_cols] = np.random.randn(7, len(agg_query_feature_cols))
    df['uid'] = 1
    df["index"] = df.index

    return df


def test_estimator(df, df_query):
    binary_dir = os.path.dirname(__file__) + '/tmp'
    os.makedirs(binary_dir, exist_ok=True)
    preprocessor = CVRPreprocessor(
        label_encoder_writer=write_label_encoder(binary_dir),
        label_encoder_reader=read_label_encoder(binary_dir)
    )

    prepr_df = preprocessor.preprocess(df, df_query, is_train=True)
    prepr_df['ad_id_weekly_sum_clicks'] = 10
    prepr_df['ad_id_monthly_sum_conversions'] = 10

    estimator = CVREstimator(
        model_writer=write_model(binary_dir),
        model_reader=read_model(binary_dir),
        output=None,
        is_tune=False)
    estimator.fit(prepr_df)

    today = datetime.datetime.strptime("2021-01-07", '%Y-%m-%d')
    result_df = estimator.predict(prepr_df, today)

    for col in OUTPUT_COLUMNS:
        assert col in result_df.columns

    # binファイルを削除
    shutil.rmtree(binary_dir)
