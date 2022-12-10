import pandas as pd

from .config import (
    OPTIMIZATION_PURPOSE_TO_PURPOSE,
    NULL_PURPOSE_DEFAULT,
    PREPROCESS_COLUMNS,
    PREPROCESS_DAILY_COLUMNS,
)

INPUT_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "campaign_id",
    "date",
    "optimization_costs",
    'minimum_daily_budget',
    'maximum_daily_budget',
    'purpose',
    'mode',
    'yesterday_target_cost',
    'remaining_days',
    'ideal_target_cost',
    'target_cost',
    'noboost_target_cost',
    'today_coefficient',
    'yesterday_coefficient',
    'C',
    "unit_weekly_ema_costs",
    "campaign_weekly_ema_costs",
    "campaign_observed_C_yesterday_in_month",
    "daily_budget",
    "unit_ex_observed_C",
]

INPUT_DAILY_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "campaign_id",
    "date",
    "clicks",
    "conversions",
    "sales",
    "costs",
    "cumsum_costs",
    "weight",
]


def to_purpose(df: pd.DataFrame) -> pd.Series:
    purpose = df['optimization_purpose'].map(
        OPTIMIZATION_PURPOSE_TO_PURPOSE
    ).fillna(NULL_PURPOSE_DEFAULT)
    return purpose


class CAPPreprocessor:
    def __init__(self, today):
        self._today = today

    def transform(self, _df: pd.DataFrame, _daily_df: pd.DataFrame) -> pd.DataFrame:
        if len(_df) == 0 or len(_daily_df) == 0:
            return pd.DataFrame(columns=PREPROCESS_COLUMNS), pd.DataFrame(columns=PREPROCESS_DAILY_COLUMNS)

        df = _df.copy()
        daily_df = _daily_df.copy()
        df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())
        df['date'] = pd.to_datetime(df['date'])
        daily_df['portfolio_id'] = daily_df['portfolio_id'].astype(pd.Int64Dtype())
        daily_df['date'] = pd.to_datetime(daily_df['date'])

        # 処理対象データを定義
        df = self._comp_df(df, daily_df)

        # 処理用データ（処理日当日扱い）
        df = df[df["date"] == self._today]
        df = df.rename(columns={
            "costs": "yesterday_costs",
            "daily_budget": "yesterday_daily_budget",
            "target_cost": "today_target_cost",
            "noboost_target_cost": "today_noboost_target_cost",
        })

        return df[PREPROCESS_COLUMNS], daily_df[PREPROCESS_DAILY_COLUMNS]

    def _comp_df(self, df: pd.DataFrame, daily_df: pd.DataFrame):
        # daily_dfのほうに持っているunit単位の情報を書き出し
        last_day = daily_df["date"].max()
        if last_day.month != self._today.month:
            df["used_costs"] = 0
        else:
            tmp_unit_info_df = daily_df[
                daily_df["date"] == last_day
            ][["advertising_account_id", "portfolio_id", "cumsum_costs"]].groupby(
                ["advertising_account_id", "portfolio_id"], dropna=False).last().reset_index()
            df = pd.merge(
                df, tmp_unit_info_df, how="left", on=["advertising_account_id", "portfolio_id"])
            df = df.rename(columns={
                "cumsum_costs": "used_costs",
            })

        # daily_dfのほうに持っているcampaign単位の情報を書き出し
        tmp_campaign_info_df = daily_df.groupby(
            ["advertising_account_id", "portfolio_id", "campaign_id"], dropna=False).last().reset_index()
        df = pd.merge(
            df, tmp_campaign_info_df, how="left", on=["advertising_account_id", "portfolio_id", "campaign_id"],
            suffixes=["", "_tmp"])

        return df
