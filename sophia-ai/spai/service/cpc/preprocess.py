import logging
import math
import pandas as pd
from typing import Callable
import importlib

from spai.utils.kpi import (
    merge_cutoff_feats,
    calc_kpis,
    calc_lag,
    calc_mean,
    calc_placement_kpi,
)

from spai.ai.preprocess import LabelEncoder

from .config import (
    TARGET_COLUMN,
    FEATURE_COLUMNS,
    WEIGHT_COLUMN,
    LAG_TERM,
    LAG_FEATURE_COLS,
    LAG_AGG_KEY_COLS_MAP,
)

logger = logging.getLogger(__name__)

agg_feature_cols = ['impressions', 'clicks',
                    'costs', 'conversions', 'sales']
agg_query_feature_cols = []


def get_dtypes():
    input_dtypes = {
        'advertising_account_id': 'int64',
        'portfolio_id': 'Int64',
        'ad_group_id': 'int64',
        'campaign_id': 'int64',
        'ad_type': 'string',
        'ad_type_feature': 'string',
        'ad_id': 'int64',
        'date': 'datetime64[ns]',
        'bidding_price': 'float64',
        'impressions': 'float64',
        'clicks': 'float64',
        'costs': 'float64',
        'conversions': 'float64',
        'sales': 'float64',
        'match_type': 'string',
        'campaign_type': 'string',
        'targeting_type': 'string',
        'budget_type': 'string',
        'account_type': 'string',
        'optimization_purpose': 'int64',
        'unit_id': 'string',
    }
    output_dtypes = [
        ('advertising_account_id', 'int64'),
        ('portfolio_id', 'Int64'),
        ('ad_group_id', 'int64'),
        ('campaign_id', 'int64'),
        ('ad_type', 'string'),
        ('ad_type_feature', 'string'),
        ('ad_id', 'int64'),
        ('date', 'datetime64[ns]'),
        ('impressions', 'float64'),
        ('clicks', 'float64'),
        ('costs', 'float64'),
        ('conversions', 'float64'),
        ('sales', 'float64'),
        ('match_type', 'string'),
        ('campaign_type', 'string'),
        ('targeting_type', 'string'),
        ('unit_id', 'string'),
        ('ad_id_weekly_sum_clicks', 'float64'),
        ('ad_id_monthly_sum_conversions', 'float64'),
        ('ad_id_monthly_sum_sales', 'float64'),
        ('ctr', 'float64'),
        ('cvr', 'float64'),
        ('rpc', 'float64'),
        ('spa', 'float64'),
        ('cpc', 'float64'),
    ]
    agg_dtypes = [
        ('weekly_impressions', 'float64'),
        ('weekly_clicks', 'float64'),
        ('weekly_costs', 'float64'),
        ('weekly_conversions', 'float64'),
        ('weekly_sales', 'float64'),
        ('weekly_ctr', 'float64'),
        ('weekly_cvr', 'float64'),
        ('weekly_rpc', 'float64'),
        ('monthly_impressions', 'float64'),
        ('monthly_clicks', 'float64'),
        ('monthly_costs', 'float64'),
        ('monthly_conversions', 'float64'),
        ('monthly_sales', 'float64'),
        ('monthly_ctr', 'float64'),
        ('monthly_cvr', 'float64'),
        ('monthly_rpc', 'float64')
    ]

    for prefix in ["ad_id", "campaign_id", "unit_id"]:
        for col_name, dtype in agg_dtypes:
            column = f"{prefix}_{col_name}"
            if any([feature_column.endswith(column) for feature_column in FEATURE_COLUMNS]):
                output_dtypes.append((column, dtype))

    for prefix in ["campaign_id", "unit_id"]:
        for col_name, dtype in agg_dtypes:
            column = f"diff_{prefix}_{col_name}"
            if any([feature_column.endswith(column) for feature_column in FEATURE_COLUMNS]):
                output_dtypes.append((column, dtype))

    for prefix in LAG_AGG_KEY_COLS_MAP.keys():
        for feature_col in LAG_FEATURE_COLS:
            for n in range(LAG_TERM):
                column = f"{prefix}_lag{n + 1}_{feature_col}"
                if column in FEATURE_COLUMNS:
                    output_dtypes.append((f"{prefix}_lag{n + 1}_{feature_col}", float))

    output_dtypes += [
        ('weekday', 'int64'),
    ]

    return input_dtypes, output_dtypes


def set_unit_id(df):
    df["portfolio_id"] = df["portfolio_id"].astype("Int64")
    df["unit_id"] = df["advertising_account_id"].astype(str) + "_" "portfolio_id_" + df["portfolio_id"].astype(str)

    return df


def agg(df: pd.DataFrame, output_dtypes: dict) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    # 足切り用変数
    df = merge_cutoff_feats(df)

    # 目的変数
    df = calc_kpis(df)

    # 説明変数
    for prefix, key_columns in LAG_AGG_KEY_COLS_MAP.items():
        df = calc_mean(df, LAG_FEATURE_COLS, key_columns=key_columns, prefix=prefix)

    for prefix, key_columns in LAG_AGG_KEY_COLS_MAP.items():
        for lag_feature in LAG_FEATURE_COLS:
            df = calc_lag(
                df,
                f"{prefix}_{lag_feature}",
                days=LAG_TERM,
                key_columns=key_columns,
                col_name_format=f"{prefix}_lag{{day}}_{lag_feature}")

    df['weekday'] = df['date'].dt.dayofweek

    df = df.drop(
        set(df.columns) ^ set([d[0] for d in output_dtypes]),
        axis=1)

    order_columns = [d[0] for d in output_dtypes]
    df = df[order_columns].reindex(columns=order_columns)

    return df


class CPCPreprocessor:
    def __init__(self,
                 label_encoder_writer: Callable[[LabelEncoder, str], None],
                 label_encoder_reader: Callable[[str], LabelEncoder],
                 output: Callable[[pd.DataFrame], None] = None,
                 batch_size: int = 8,
                 is_use_dask: bool = False,):
        self._output = output
        self._label_encoder_writer = label_encoder_writer
        self._label_encoder_reader = label_encoder_reader
        self._batch_size = batch_size
        self._is_use_dask = is_use_dask

    def _preprocess(self, df, df_placement, dask_util=None):
        df = set_unit_id(df)
        for col in agg_feature_cols:
            df[col] = df[col].fillna(0)
        df["optimization_purpose"] = df["optimization_purpose"].fillna(0)

        input_dtypes, output_dtypes = get_dtypes()
        df = df.astype(input_dtypes)
        if self._is_use_dask:
            npartitions = math.ceil(len(df["uid"].unique()) / self._batch_size)
            df = df.set_index("uid")
            df = dask_util.dask_dataframe_from_pandas(df, npartitions=npartitions)  # pandas -> dask
            df = self._client.persist(df)

            meta = dask_util.make_meta(output_dtypes)
            df = df.map_partitions(agg, output_dtypes, meta=meta)
            df = df.clear_divisions()
            df = df.compute()  # dask -> pandas
        else:
            df = agg(df, output_dtypes)

        df = calc_placement_kpi(df, df_placement)

        return df

    def add_catcodes(self, df: pd.DataFrame, is_train: bool) -> pd.DataFrame:
        cat_columns = df.select_dtypes(exclude=['number', 'datetime']).columns
        for col in cat_columns:
            print(col)
            if col in ["unit_id", "ad_type"]:
                continue
            if is_train:
                le = LabelEncoder()
                le.fit(df[col].values)
                self._label_encoder_writer(le.to_bytes(), col)
            else:
                le = LabelEncoder.from_bytes(self._label_encoder_reader(col))
            df[col] = le.transform(df[col].values)

        df = df.sort_values('date')
        return df

    def _preprocess_dask(self, df: pd.DataFrame, df_placement: pd.DataFrame, is_train: bool):
        dask_util = importlib.import_module("spai.utils.dask")
        self._client, self._cluster = dask_util.get_dask_client()

        logger.info("start _preprocess")
        df = self._preprocess(df, df_placement, dask_util)
        logger.info("finished _preprocess")

        logger.info("start add_catcodes")
        df = self.add_catcodes(df, is_train)
        logger.info("finished add_catcodes")

        df["weight"] = df[WEIGHT_COLUMN].fillna(0)

        df = df.drop(agg_feature_cols, axis=1)
        df.reset_index(drop=True, inplace=True)

        self._cluster.close()
        self._client.close()
        return df

    def _preprocess_pandas(self, df: pd.DataFrame, df_placement: pd.DataFrame, is_train: bool):

        logger.info("start preprocess_ad")
        df = self._preprocess(df, df_placement)
        logger.info("finished preprocess_ad")

        logger.info("start add_catcodes")
        df = self.add_catcodes(df, is_train)
        logger.info("finished add_catcodes")

        df["weight"] = df[WEIGHT_COLUMN].fillna(0)

        df = df.drop(agg_feature_cols, axis=1)
        df.reset_index(drop=True, inplace=True)

        return df

    def preprocess(self, df: pd.DataFrame, df_placement: pd.DataFrame = None, is_train: bool = False) -> pd.DataFrame:
        df["ad_type_feature"] = df["ad_type"]
        if self._is_use_dask:
            df = self._preprocess_dask(df, df_placement, is_train)
        else:
            df = self._preprocess_pandas(df, df_placement, is_train)

        if is_train:
            df[TARGET_COLUMN] = df.groupby([
                'portfolio_id',
                'advertising_account_id',
                'campaign_id',
                'ad_type',
                'ad_id'], dropna=False)[TARGET_COLUMN].shift(-1)

        if callable(self._output):
            self._output(df)

        return df
