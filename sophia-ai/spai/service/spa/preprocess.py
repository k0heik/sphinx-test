import logging
import math
import pandas as pd
from typing import Callable
import importlib

from spai.utils.kpi import (
    merge_feats,
    merge_agg_feats,
    merge_cutoff_feats,
    calc_kpis,
    safe_div,
    ewm,
    calc_lag,
    calc_mean,
)
from spai.ai.preprocess import LabelEncoder

from .config import (
    FEATURE_COLUMNS,
    WEIGHT_COLUMN,
    LAG_TERM,
    LAG_FEATURE_COLS,
    LAG_AGG_KEY_COLS_MAP,
)


logger = logging.getLogger(__name__)

agg_feature_cols = ['impressions', 'clicks',
                    'costs', 'conversions', 'sales']
agg_query_feature_cols = ['query_clicks', 'query_conversions']
KPI_COLUMNS = [
    "ewm7_impressions",
    "ewm7_clicks",
    "ewm7_costs",
    "ewm7_conversions",
    "ewm7_sales",
    "ewm7_ctr",
    "ewm7_cvr",
    "ewm7_rpc",
    "weekly_impressions",
    "weekly_clicks",
    "weekly_costs",
    "weekly_conversions",
    "weekly_sales",
    "weekly_ctr",
    "weekly_cvr",
    "weekly_rpc",
    "monthly_impressions",
    "monthly_clicks",
    "monthly_costs",
    "monthly_conversions",
    "monthly_sales",
    "monthly_ctr",
    "monthly_cvr",
    "monthly_rpc",
    "monthly_spa"
]


def get_dtypes():
    input_dtypes = {
        'advertising_account_id': 'int64',
        'portfolio_id': 'Int64',
        'ad_group_id': 'int64',
        'campaign_id': 'int64',
        'ad_type': 'string',
        'ad_id': 'int64',
        'date': 'datetime64[ns]',
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
        'unit_id': 'object',
        'query_clicks': 'float64',
        'query_conversions': 'float64',
    }
    output_dtypes = [
        ('advertising_account_id', 'int64'),
        ('portfolio_id', 'Int64'),
        ('ad_group_id', 'int64'),
        ('campaign_id', 'int64'),
        ('ad_type', 'string'),
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
        ('budget_type', 'string'),
        ('account_type', 'string'),
        ('optimization_purpose', 'int64'),
        ('unit_id', 'string'),
        ('query_clicks', 'float64'),
        ('query_conversions', 'float64'),
        ('ad_id_ewm7_query_conversions', 'float64'),
        ('ad_id_ewm28_query_conversions', 'float64'),
        ('ad_id_weekly_sum_clicks', 'float64'),
        ('ad_id_monthly_sum_conversions', 'float64'),
        ('ad_id_monthly_sum_sales', 'float64'),
        ('ctr', 'float64'),
        ('cvr', 'float64'),
        ('rpc', 'float64'),
        ('spa', 'float64'),
    ]
    agg_dtypes = [
        ('ewm7_impressions', 'float64'),
        ('ewm7_clicks', 'float64'),
        ('ewm7_costs', 'float64'),
        ('ewm7_conversions', 'float64'),
        ('ewm7_sales', 'float64'),
        ('ewm7_ctr', 'float64'),
        ('ewm7_cvr', 'float64'),
        ('ewm7_rpc', 'float64'),
        ('ewm7_spa', 'float64'),
        ('weekly_impressions', 'float64'),
        ('weekly_clicks', 'float64'),
        ('weekly_costs', 'float64'),
        ('weekly_conversions', 'float64'),
        ('weekly_sales', 'float64'),
        ('weekly_ctr', 'float64'),
        ('weekly_cvr', 'float64'),
        ('weekly_rpc', 'float64'),
        ('weekly_spa', 'float64'),
        ('monthly_impressions', 'float64'),
        ('monthly_clicks', 'float64'),
        ('monthly_costs', 'float64'),
        ('monthly_conversions', 'float64'),
        ('monthly_sales', 'float64'),
        ('monthly_ctr', 'float64'),
        ('monthly_cvr', 'float64'),
        ('monthly_rpc', 'float64'),
        ('monthly_spa', 'float64')
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

    output_dtypes += [('weekday', 'int64'), ('day_of_month', 'int64')]

    return input_dtypes, output_dtypes


def set_unit_id(df):
    df["portfolio_id"] = df["portfolio_id"].astype("Int64")
    df["unit_id"] = df["advertising_account_id"].astype(str) + "_" "portfolio_id_" + df["portfolio_id"].astype(str)

    return df


def kpi_diff(df):
    print(df.columns)
    for prefix in ["campaign_id", "unit_id"]:
        for col_name in KPI_COLUMNS:
            column = f"diff_{prefix}_{col_name}"
            if any([feature_column.endswith(column) for feature_column in FEATURE_COLUMNS]):
                df[column] = safe_div(
                    (df[f"ad_id_{col_name}"] - df[f"{prefix}_{col_name}"]), df[f"{prefix}_{col_name}"])

    return df


def merge_query_feats(df, freq='7D', prefix='weekly', key_column='ad_id'):
    """クエリ用の特徴量の計算
    """
    min_periods = 1
    df['index'] = df.index

    df = df.sort_values([key_column, 'date'])
    res = df[[key_column, "date"] + agg_query_feature_cols]
    res = res.sort_values([key_column, 'date'])
    dates = res['date'].values
    ids = res[key_column].values
    res = res.fillna(0.0).groupby(key_column, group_keys=True)

    res = ewm(res, freq, min_periods, agg_query_feature_cols)

    if key_column in res.columns:
        res = res.drop([key_column], axis=1)

    if 'date' in res.columns:
        res = res.drop(['date'], axis=1)

    res = res.fillna(0.0).reset_index().rename(columns={'level_1': 'index'})
    # ewmの場合にdateとad_idがindexに含まれない
    if 'date' not in res.columns:
        res['date'] = dates
    if key_column not in res.columns:
        res[key_column] = ids
    res[key_column] = res[key_column].astype(int)
    df = merge_feats(df, res, prefix=prefix, key_column=key_column)
    df = df.sort_values('index').reset_index(drop=True)
    drop_columns = []
    for column in agg_query_feature_cols:
        drop_column = f"{key_column}_{prefix}_{column}"
        if drop_column not in FEATURE_COLUMNS:
            drop_columns.append(drop_column)
    df = df.drop(['index'] + drop_columns, axis=1)
    return df


def agg(df: pd.DataFrame, output_dtypes: dict) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    # 足切り用変数
    df = merge_cutoff_feats(df)

    # クエリ用変数
    df = merge_query_feats(df, freq='7D', prefix='ewm7')
    df = merge_query_feats(df, freq='28D', prefix='ewm28')

    # 目的変数
    df = calc_kpis(df)

    # 説明変数
    df = merge_agg_feats(
        df, '7D', 'ewm', prefix='ewm7', key_column='ad_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '7D', 'rolling', prefix='weekly', key_column='ad_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '28D', 'rolling', prefix='monthly', key_column='ad_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '7D', 'ewm', prefix='ewm7', key_column='campaign_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '7D', 'rolling', prefix='weekly', key_column='campaign_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '28D', 'rolling', prefix='monthly', key_column='campaign_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '7D', 'ewm', prefix='ewm7', key_column='unit_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '7D', 'rolling', prefix='weekly', key_column='unit_id', feature_columns=FEATURE_COLUMNS)
    df = merge_agg_feats(
        df, '28D', 'rolling', prefix='monthly', key_column='unit_id', feature_columns=FEATURE_COLUMNS)
    df = kpi_diff(df)

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
    df['day_of_month'] = df['date'].dt.day

    df = df.drop(
        set(df.columns) ^ set([d[0] for d in output_dtypes]),
        axis=1)

    order_columns = [d[0] for d in output_dtypes]
    df = df[order_columns].reindex(columns=order_columns)

    return df


def merge_query_df(df: pd.DataFrame, df_query: pd.DataFrame):
    # キャンペーン & クエリ単位で集計
    df_sum_query = df_query.groupby(
        ['query', 'campaign_id', 'date']
    )[agg_query_feature_cols].sum(numeric_only=True).reset_index()

    for col in agg_query_feature_cols:
        if col not in df_sum_query.columns:
            df_sum_query[col] = 0

    df_query.drop(agg_query_feature_cols, axis=1, inplace=True)
    # 集計結果をキーワード単位で紐つけ
    df_query = df_query.merge(
        df_sum_query[
            ['campaign_id', 'query', 'date',
             'query_clicks', 'query_conversions']
        ],
        on=['campaign_id', 'query', 'date']
    )
    df_query_keyword = df_query.groupby(
        ["ad_type", "ad_id", "date"]
    )[agg_query_feature_cols].sum().reset_index()
    df = pd.merge(
        df,
        df_query_keyword,
        how='left',
        on=["ad_type", "ad_id", "date"]
    )
    return df


class SPAPreprocessor:
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

    def _preprocess(self, df, df_query, dask_util=None):
        df = set_unit_id(df)
        for col in agg_feature_cols:
            df[col] = df[col].fillna(0)
        df["optimization_purpose"] = df["optimization_purpose"].fillna(0)

        df_query["portfolio_id"] = df_query["portfolio_id"].astype(pd.Int64Dtype())

        df = merge_query_df(df, df_query)

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

        return df

    def add_catcodes(self, df: pd.DataFrame, is_train: bool) -> pd.DataFrame:
        cat_columns = df.select_dtypes(exclude=['number', 'datetime']).columns
        for col in cat_columns:
            # ad_typeは特徴量として使用しないので、コード化しない
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

    def _preprocess_dask(self, df: pd.DataFrame, df_query: pd.DataFrame, is_train: bool):
        dask_util = importlib.import_module("spai.utils.dask")
        self._client, self._cluster = dask_util.get_dask_client()

        logger.info("start _preprocess")
        df = self._preprocess(df, df_query, dask_util)
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

    def _preprocess_pandas(self, df: pd.DataFrame, df_query: pd.DataFrame, is_train: bool):

        logger.info("start preprocess_ad")
        df = self._preprocess(df, df_query)
        logger.info("finished preprocess_ad")

        logger.info("start add_catcodes")
        df = self.add_catcodes(df, is_train)
        logger.info("finished add_catcodes")

        df["weight"] = df[WEIGHT_COLUMN].fillna(0)

        df = df.drop(agg_feature_cols, axis=1)
        df.reset_index(drop=True, inplace=True)

        return df

    def preprocess(self, df: pd.DataFrame, df_query: pd.DataFrame, is_train: bool) -> pd.DataFrame:

        if self._is_use_dask:
            df = self._preprocess_dask(df, df_query, is_train)
        else:
            df = self._preprocess_pandas(df, df_query, is_train)

        if callable(self._output):
            self._output(df)

        return df
