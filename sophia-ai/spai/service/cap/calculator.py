import datetime
import calendar
import numpy as np
import pandas as pd
from logging import getLogger

from spai.utils.kpi import safe_div, calc_unit_weekly_cpc_for_cap
from spai.utils.kpi.kpi import (
    MODE_KPI
)
from spai.optim.utils import weighted_ma
from .config import (
    UNIT_PK_COLS,
    OUTPUT_COLUMNS,
    COSTS_WINDOW_SIZE,
    CLICK_WINDOW_SIZE,
    CV_WINDOW_SIZE,
    SALES_WINDOW_SIZE,
)


logger = getLogger(__name__)


_TARGET_CRITERION_DAYS = 7
_POTENTIAL_THRESHOLD = 0.8
_UPPER_RATIO_BOUNDS = 2.0
_UPPER_RATIO_BOUNDS_NOT_POTENTIAL = 1.2
_UPPER_RATIO_BOUNDS_CPC_NOT_POTENTIAL = 2.0


class CAPCalculator():

    def __init__(self, today) -> None:
        self._today = today

    def calc(
        self, df: pd.DataFrame, daily_df: pd.DataFrame, campaign_all_actual_df: pd.DataFrame
    ) -> pd.DataFrame:
        df, daily_df = self._clean(df, daily_df)
        if len(df) == 0 or len(daily_df) == 0:
            logger.error("calculatable data does not exist.")
            return None

        df["unit_weekly_cpc_for_cap"] = calc_unit_weekly_cpc_for_cap(campaign_all_actual_df, self._today)

        # 当月のユニット予算を使い切っている場合、最低日予算を設定
        if all(df["today_target_cost"] <= 0):
            df['daily_budget_upper'] = df['minimum_daily_budget']
        # ユニットの過去１週間のCPCが0の場合、日予算は前日から変更なし
        elif all(df["unit_weekly_cpc_for_cap"] == 0):
            df['is_daily_budget_undecidable_unit'] = True
            df['daily_budget_upper'] = df["yesterday_daily_budget"]
        # 日予算計算
        else:
            df['is_daily_budget_undecidable_unit'] = False
            df['weight'] = self._init_weight(df, daily_df, campaign_all_actual_df)
            df['value_of_campaign'] = self._calc_value_of_campaign(df, daily_df)
            df = self._update_weight(df)
            df = self._calc_daily_budget_upper(df, daily_df)
            df = self._clip(df)

        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df["daily_budget_upper"] = df["daily_budget_upper"].astype(int)

        return df[OUTPUT_COLUMNS]

    def _ema(self, x, window_size):
        return weighted_ma(x, window_size)[-1]

    def _clean(self, _df: pd.DataFrame, _daily_df: pd.DataFrame) -> pd.DataFrame:
        df = _df.copy()
        df["date"] = pd.to_datetime(df["date"])

        daily_df = _daily_df.copy()
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        daily_df = daily_df[daily_df["date"] < self._today]
        daily_df['clicks'] = daily_df['clicks'].fillna(0).astype(float)
        daily_df['conversions'] = daily_df['conversions'] \
            .fillna(0).astype(float)
        daily_df['sales'] = daily_df['sales'].fillna(0).astype(float)
        daily_df['costs'] = daily_df['costs'].fillna(0).astype(float)
        daily_df = daily_df.sort_values([
            "advertising_account_id",
            "portfolio_id",
            "campaign_id",
            "date"
        ])

        target_campaign_id_df = pd.Series(daily_df[
            (daily_df["date"] < self._today)
            & (daily_df["date"] >= (self._today - datetime.timedelta(days=_TARGET_CRITERION_DAYS)))
        ]["campaign_id"].unique(), name="campaign_id")

        df = pd.merge(
            df, target_campaign_id_df, how="inner", on="campaign_id")
        daily_df = pd.merge(
            daily_df, target_campaign_id_df, how="inner", on="campaign_id")

        df = df.sort_values(["advertising_account_id", "portfolio_id", "campaign_id", "date"])
        daily_df = daily_df.sort_values(["advertising_account_id", "portfolio_id", "campaign_id", "date"])

        df = df.reset_index(drop=True)
        daily_df = daily_df.reset_index(drop=True)

        return df, daily_df

    def _merge_ema(
        self,
        df: pd.DataFrame,
        actual_df: pd.DataFrame,
        window_size: int,
        col: str,
        name: str,
        include_campaign: bool = False,
    ) -> pd.DataFrame:
        pk = [
            "advertising_account_id",
            "portfolio_id",
        ]
        if include_campaign:
            pk.append("campaign_id")

        agg = actual_df.groupby(
            pk + ['date'], dropna=False)[col].sum().reset_index()
        val = agg.groupby(pk, dropna=False).apply(
            lambda x: self._ema(
                x.sort_values("date")[col].values,
                window_size
            )
        ).rename(name).reset_index()
        df = pd.merge(
            df, val,
            on=pk,
            how='left'
        )
        return df

    def _merge_max(
        self,
        df: pd.DataFrame,
        daily_df: pd.DataFrame,
        window_size: int,
        col: str,
        name: str,
    ) -> pd.DataFrame:
        pk = [
            "advertising_account_id",
            "portfolio_id",
            "campaign_id"
        ]

        daily_df = daily_df.sort_values(pk + ['date'])
        agg = daily_df.groupby(
            pk + ['date'], dropna=False)[col].sum().reset_index()
        val = agg.groupby(pk, dropna=False)\
            .rolling(window_size, min_periods=1)[col].max()\
            .rename(name).reset_index()

        val = val.groupby(pk, dropna=False).last().reset_index()
        df = pd.merge(
            df, val,
            on=pk,
            how='left'
        )
        return df

    def _init_weight_normalize(self, df, column, aggregation_keys=["advertising_account_id", "portfolio_id"]):
        agg = df.groupby(aggregation_keys, dropna=False)[column] \
            .sum().rename('weight_sum').reset_index()
        df = pd.merge(df, agg, on=aggregation_keys, how='left')

        result = df[column] / df['weight_sum']
        return result

    def _init_weight(
        self,
        df: pd.DataFrame,
        daily_df: pd.DataFrame,
        campaign_all_actual_df: pd.DataFrame,
    ) -> pd.Series:

        df['prev_weight'] = df['weight'].fillna(0.0)

        # count
        count = df.groupby(UNIT_PK_COLS, dropna=False)['date'] \
            .count().rename('count').reset_index()
        df = pd.merge(df, count, on=UNIT_PK_COLS, how='left')
        count = df['count'].values.astype(np.float64)
        count_weight = 1 / count

        df = self._merge_ema(
            df, daily_df, COSTS_WINDOW_SIZE, "costs", "_ema_costs", include_campaign=True)
        df = self._merge_ema(
            df, campaign_all_actual_df, COSTS_WINDOW_SIZE, "costs", "unit_costs", include_campaign=False)
        df = self._merge_ema(
            df, campaign_all_actual_df, CLICK_WINDOW_SIZE, "clicks", "unit_clicks", include_campaign=False)

        df["unit_cpc"] = safe_div(
            df["unit_costs"].values, df["unit_clicks"].values)

        # キャンペーンの広告費用の移動平均での重みの算出
        _ema_costs = df["_ema_costs"].fillna(0).values.astype(np.float64)
        unit_costs = df["unit_costs"].fillna(0).values.astype(np.float64)
        ema_cost_weight = safe_div(_ema_costs, unit_costs)

        weight = np.where(
            # 前回の重みがあるか（ある場合はそのまま）
            ~pd.isnull(df['weight']),
            df['weight'],
            np.where(
                _ema_costs > 0,
                ema_cost_weight,
                count_weight
            )
        )
        df['weight'] = weight

        assert not df['weight'].isnull().any()

        nowmalized_weight = self._init_weight_normalize(df, 'weight')

        assert not pd.isnull(nowmalized_weight).any()

        return pd.Series(nowmalized_weight, index=df.index)

    def _calc_value_of_campaign(
        self,
        df: pd.DataFrame,
        daily_df: pd.DataFrame
    ) -> pd.Series:
        assert (~df['weight'].isnull()).all(), 'Initialize weights before call.'

        df = self._merge_ema(
            df, daily_df, CLICK_WINDOW_SIZE, "clicks", "_ema_clicks", include_campaign=True)
        df = self._merge_ema(
            df, daily_df, CV_WINDOW_SIZE, "conversions", "_ema_conversions", include_campaign=True)
        df = self._merge_ema(
            df, daily_df, SALES_WINDOW_SIZE, "sales", "_ema_sales", include_campaign=True)
        df = self._merge_ema(
            df, daily_df, 7, "costs", "_ema7_costs", include_campaign=True)
        df = self._merge_ema(
            df, daily_df, 28, "costs", "_ema28_costs", include_campaign=True)

        kpi = np.where(
            df['purpose'] == 'CLICK',
            df["_ema_clicks"],
            np.where(
                df['purpose'] == 'CONVERSION',
                df["_ema_conversions"],
                np.where(
                    df['purpose'] == 'SALES',
                    df["_ema_sales"],
                    np.nan,
                )
            )
        )
        if np.any(np.isnan(kpi)):
            raise NotImplementedError(
                'There exist some purposes not in {CLICK, CONVERSION, SALES}')

        costs = np.where(
            df['purpose'] == 'CLICK',
            df["_ema7_costs"],
            df["_ema28_costs"],
        )
        virtual_cost = df['weight'] * df['yesterday_target_cost']
        c = safe_div(virtual_cost, costs).clip(max=1)
        value_of_campaign = np.where(
            costs > 0,
            kpi * c,
            0.0
        ).astype(np.float64)
        return pd.Series(value_of_campaign, index=df.index)

    def _gradient(
        self,
        df: pd.DataFrame, l2_lambda: float = 1e-1, alpha: float = 1e-1
    ) -> np.ndarray:
        assert 'value_of_campaign' in df.columns, 'Calc values before call.'
        count = df.groupby(UNIT_PK_COLS, dropna=False)['date'] \
            .count().rename('count').reset_index()
        df = pd.merge(df, count, on=UNIT_PK_COLS, how='left')
        df['u'] = 1 / df['count'].values

        # TODO: 前日予算あとで差し替え
        df['b'] = df['weight'] * df['yesterday_target_cost']
        df['q'] = np.where(df['b'] > 0, safe_div(df['value_of_campaign'], df['b']), 0)
        assert np.all(df['q'] >= 0)

        # ユニットごとにmax_qを計算
        max_q = df.groupby(UNIT_PK_COLS, dropna=False)['q'] \
            .max().rename('max_q').reset_index()
        df = pd.merge(df, max_q, on=UNIT_PK_COLS, how='left')
        # max_qが0の場合はg=0として更新しない。
        df['gradient'] = np.where(df['max_q'] > 0, - safe_div(df['q'], df['max_q']) + l2_lambda * (
            df['weight'] - df['u']), 0.0)

        df['p'] = np.exp(- alpha * df['gradient'].astype(float))

        return df

    def _update_weight(self, df: pd.DataFrame) -> pd.Series:
        df = self._gradient(df)
        df['weight'] *= df["p"]
        sum_weight = df.groupby(UNIT_PK_COLS, dropna=False)['weight'] \
            .sum().rename('sum_weight').reset_index()
        assert (sum_weight['sum_weight'] > 0).all()
        df = pd.merge(df, sum_weight, on=UNIT_PK_COLS, how='left')
        df['weight'] /= df['sum_weight']
        return df

    def _clip_bad_performance(self, df):
        # 成績が悪い場合は前日よりも高くしない
        if self._today.day != 1 and df["C"].values[0] is not None:
            df["daily_budget_upper"] = np.where(
                (
                    (
                        ((df["mode"] == MODE_KPI) & (df["unit_ex_observed_C"] > df["C"]))
                        | (
                            (df["mode"] != MODE_KPI) & (df["today_target_cost"] < df["ideal_target_cost"])
                            & (df["unit_ex_observed_C"] > df["C"])
                        )
                    ) & (
                        (df["campaign_weekly_ema_costs"] > df["unit_weekly_ema_costs"] * 0.1)
                        & (df["campaign_observed_C_yesterday_in_month"] > df["C"])
                    ) & ~df["yesterday_daily_budget"].isnull()
                ),
                np.minimum(
                    df["yesterday_daily_budget"],
                    df["daily_budget_upper"]
                ),
                df["daily_budget_upper"]
            )

        return df

    def _calc_daily_budget_upper(
        self,
        df: pd.DataFrame,
        daily_df: pd.DataFrame,
        potential_threshold: float = _POTENTIAL_THRESHOLD,
        upper_ratio_bounds: float = _UPPER_RATIO_BOUNDS,
        upper_ratio_bounds_not_potential: float = _UPPER_RATIO_BOUNDS_NOT_POTENTIAL,
        upper_ratio_bounds_cpc_not_potential: float = _UPPER_RATIO_BOUNDS_CPC_NOT_POTENTIAL,

    ) -> pd.Series:
        assert (~df['weight'].isnull()).all(), 'Update weights before call.'
        assert 'unit_weekly_cpc_for_cap' in df.columns, 'Calced unit_weekly_cpc_for_cap before call.'

        df = self._merge_max(
            df, daily_df, COSTS_WINDOW_SIZE, "costs", "last_week_max_costs")

        df["origin_daily_budget_upper"] = df["weight"] * df["today_noboost_target_cost"]
        df["boosted_origin_daily_budget_upper"] = df["weight"] * df["today_target_cost"]

        df["has_potential"] = np.logical_not(
            (
                (df["yesterday_costs"] <
                    df["yesterday_daily_budget"] * df["yesterday_coefficient"] * potential_threshold)
                | (df["yesterday_daily_budget"].isnull())
            )
            & (df["yesterday_costs"] < df["origin_daily_budget_upper"])
        )
        df["expected_costs"] = np.where(
            df["has_potential"],
            df["boosted_origin_daily_budget_upper"],
            df["yesterday_costs"],
        ).astype(float)
        total_expected_cost = df.groupby([
            "advertising_account_id",
            "portfolio_id"
        ], dropna=False)["expected_costs"].sum() \
            .rename("total_expected_cost").reset_index()
        df = pd.merge(
            df, total_expected_cost,
            on=["advertising_account_id", "portfolio_id"], how="left"
        )

        df["costs_hat"] = np.where(
            df["has_potential"],
            df["origin_daily_budget_upper"],
            df["yesterday_costs"],
        ).astype(float)
        costs_hat_sum = df.groupby([
            "advertising_account_id",
            "portfolio_id"
        ], dropna=False)["costs_hat"].sum() \
            .rename("costs_hat_sum").reset_index()
        df = pd.merge(
            df, costs_hat_sum,
            on=["advertising_account_id", "portfolio_id"],
            how="left"
        )

        df["potential_weight"] = np.where(
            df["has_potential"],
            df["weight"],
            0,
        ).astype(float)
        potential_weight_sum = df.groupby([
            "advertising_account_id",
            "portfolio_id"
        ], dropna=False)["potential_weight"].sum() \
            .rename("potential_weight_sum").reset_index()
        df = pd.merge(
            df, potential_weight_sum,
            on=["advertising_account_id", "portfolio_id"], how="left"
        )

        df["total_margin"] = df["today_target_cost"] - df["total_expected_cost"]
        df["noboost_total_margin"] = df["total_margin"] / df["today_coefficient"]

        df["daily_budget_upper"] = np.where(
            df["has_potential"],
            np.minimum(
                upper_ratio_bounds * df["origin_daily_budget_upper"],
                (
                    df["origin_daily_budget_upper"]
                    + df["potential_weight"] * safe_div(df["noboost_total_margin"], df["potential_weight_sum"])
                ),
            ),
            np.maximum(
                np.minimum(
                    df["origin_daily_budget_upper"],
                    np.minimum(
                        df["yesterday_daily_budget"],
                        df["last_week_max_costs"] * upper_ratio_bounds_not_potential
                    )
                ),
                np.minimum(
                    df["yesterday_daily_budget"],
                    df["unit_weekly_cpc_for_cap"] * upper_ratio_bounds_cpc_not_potential
                )
            )
        )

        # has_potentialがないcampaignについて、weightの値を設定した日予算に応じて修正する
        df["weight"] = np.where(
            df["has_potential"],
            df["weight"],
            np.minimum(safe_div(df["daily_budget_upper"], df["today_noboost_target_cost"]), df["weight"])
        )

        return df

    def _clip(self, df, upper_ratio_bounds=_UPPER_RATIO_BOUNDS):
        # 元の値を退避
        df["base_daily_budget_upper"] = df["daily_budget_upper"]

        # 成績が悪い場合は前日よりも高くしない
        df = self._clip_bad_performance(df)

        # 月初 & 前月末
        if self._today.day == 1 and all(df["yesterday_target_cost"] == 0):
            # 月初かつ、前月の残予算が0だった時は一時的に予算の上限を緩和する(事故っても、大事にならないようにideal_budgetで抑えるようにする)
            df["daily_budget_upper"] = np.minimum(
                df["daily_budget_upper"],
                df["optimization_costs"] / calendar.monthrange(self._today.year, self._today.month)[1]
            )
        else:
            df["budget_upper_bound"] = upper_ratio_bounds * df["yesterday_daily_budget"]
            df["daily_budget_upper"] = np.minimum(df["daily_budget_upper"], df["budget_upper_bound"])

        # 設定値から上下限clip
        df["daily_budget_upper"] = np.maximum(
            df["daily_budget_upper"], df["minimum_daily_budget"])
        df["daily_budget_upper"] = np.minimum(
            df["daily_budget_upper"], df["maximum_daily_budget"])

        df["daily_budget_upper"] = df["daily_budget_upper"].astype(int)

        return df
