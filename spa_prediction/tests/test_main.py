import datetime
import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from main import cli
from spai.service.spa.preprocess import (
    agg_feature_cols,
    agg_query_feature_cols,
)


class BusinessException(Exception):
    pass


@pytest.fixture
def df():
    df = pd.DataFrame(index=np.arange(28), columns=agg_feature_cols)
    df[agg_feature_cols] = 100 * np.random.rand(28, len(agg_feature_cols))
    df["advertising_account_id"] = 1
    df["portfolio_id"] = 1
    df["ad_group_id"] = 1
    df["campaign_id"] = 1
    df["ad_type"] = "ad_type"
    df["ad_id"] = 1
    df["date"] = pd.date_range("2021-01-01", "2021-01-28")
    df["uid"] = 1
    df["date"] = df["date"].dt.strftime('%Y-%m-%d')
    return df


@pytest.fixture
def ad_df():
    df = pd.DataFrame({
        "advertising_account_id": [1] * 2,
        "portfolio_id": [1] * 2,
        "ad_group_id": [1] * 2,
        "campaign_id": [1] * 2,
        "ad_type": ["ad_type"] * 2,
        "ad_id": [1, 2]
    })
    df["match_type"] = "match_type"
    df["account_type"] = "account_type"
    df["campaign_type"] = "campaign_type"
    df["targeting_type"] = "targeting_type"
    df["budget_type"] = "budget_type"
    df["optimization_purpose"] = 1
    df["budget"] = 100000
    df["uid"] = 1

    return df


@pytest.fixture
def keyword_queries_df():
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
    df["date"] = df["date"].dt.strftime('%Y-%m-%d')
    df[agg_query_feature_cols] = 100 * np.random.rand(
        days, len(agg_query_feature_cols))

    return df


@pytest.mark.parametrize("date", ["2021-01-29", "2021-01-29T03:00:00Z"])
@pytest.mark.parametrize("is_df_zero", [True, False])
def test_train(mocker, monkeypatch, df, ad_df, keyword_queries_df, is_df_zero, date):
    def _df_data(
        path, sqlfilename, *_
    ):
        if "keyword_queries" in sqlfilename:
            return keyword_queries_df
        elif "ad_info" in sqlfilename:
            return ad_df
        else:
            if is_df_zero:
                return pd.DataFrame(columns=df.columns)
            return df

    # 学習時間がかからないparams
    params = {
        "learning_rate": 0.01,
        "depth": 1,
        "has_time": True,
        "iterations": 10,
    }
    bq = mocker.MagicMock()
    bq.extract_to_df.side_effect = _df_data
    mocker.patch("main.BigQueryService", return_value=bq)

    mocker.patch("main.write_df_to_s3", return_value=None)
    mocker.patch("main.output_preprocess_to_s3_manager", return_value=mocker.MagicMock())
    mocker.patch("main.write_latest_model_manager", return_value=mocker.MagicMock())
    mocker.patch("main.write_latest_labelencoder_manager", return_value=mocker.MagicMock())
    mocker.patch("spai.ai.boosting.get_tuned_params", return_value=params)

    mocker.patch("main.business_exception", side_effect=BusinessException)

    # mainで取得している環境変数のパッチ
    monkeypatch.setenv("DATASET_NAME", "testing")
    monkeypatch.setenv("BUCKET", "testing")
    monkeypatch.setenv("GCP_PROJECT_ID", "testing")

    # clickを使用しているため，以下の様に実行する必要がある
    runner = CliRunner()
    result = runner.invoke(cli, ["train", date])
    if is_df_zero:
        # 抽出データが0件のケース
        assert isinstance(result.exception, BusinessException)
    else:
        # 正常系
        assert result.exit_code == 0
