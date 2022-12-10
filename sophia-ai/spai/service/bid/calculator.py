import math
import traceback
import numpy as np
import pandas as pd
from logging import getLogger
from typing import Tuple
from datetime import timedelta

from spai.optim.models import (
    KPI, Purpose,
)
from spai.utils.kpi.kpi import (
    MODE_KPI
)
from spai.optim.utils import clip, weighted_cpc
from .config import (
    OptimiseTargetConfig,
    BiddingRuleConfig,
    BiddingMLConfig,
    OUTPUT_DTYPES,
    CPC_LIMIT_RATE
)

logger = getLogger(__name__)


def get_purpose(value):
    if value == "SALES":
        return Purpose.SALES
    elif value == "CONVERSION":
        return Purpose.CONVERSION
    elif value == "CLICK":
        return Purpose.CLICK
    else:
        return Purpose.SALES


def get_kpi(value):
    if value == "NULL":
        return KPI.NULL
    elif value == "ROAS":
        return KPI.ROAS
    elif value == "CPA":
        return KPI.CPA
    elif value == "CPC":
        return KPI.CPC
    else:
        return KPI.NULL


def estimate_ad_cpc(ws: int, historical_ad_df):
    clicks = historical_ad_df['clicks'].values[-ws::]
    costs = historical_ad_df['costs'].values[-ws::]
    return weighted_cpc(ws, clicks, costs)


class BIDCalculator():

    def __init__(self, today) -> None:
        self._today = today

    def _is_by_ml(self, ad_df, ws=7):
        end = self._today
        start = end - timedelta(days=ws + 1)

        # 過去一週間のクリックが0より大きい場合は，
        # PID制御による入札額調整を適用
        return (
            np.sum(ad_df.loc[
                (ad_df['date'] >= start) & (ad_df['date'] < end), 'clicks'].values)
            > OptimiseTargetConfig.THRESHOLD_OF_CLICKS_WEEKLY
        )

    def _is_provisional_bidding(self, ad_df, ws=28):
        purpose = get_purpose(ad_df['purpose'].values[-1])

        end = self._today
        start = end - timedelta(days=ws + 1)

        if purpose is Purpose.CLICK:
            return False
        elif purpose is Purpose.CONVERSION:
            # 過去28日間のコンバージョン数が0より大きくない場合は
            # 仮の入札額計算
            return not (
                np.sum(ad_df.loc[
                    (ad_df['date'] >= start) & (ad_df['date'] < end), 'conversions'].values)
                > OptimiseTargetConfig.THRESHOLD_OF_CV_MONTHLY
            )
        elif purpose is Purpose.SALES:
            # 過去28日間の売り上げが0より大きくない場合は
            # 仮の入札額計算
            return not (
                np.sum(ad_df.loc[
                    (ad_df['date'] >= start) & (ad_df['date'] < end), 'sales'].values)
                > OptimiseTargetConfig.THRESHOLD_OF_SALES_MONTHLY
            )

        raise ValueError

    def _check_abnormal(self, bid: float) -> bool:
        """異常値判定を行う。オーバーフロー時はExceptionを発生させる"""
        try:
            int(np.float64(bid))
        except OverflowError as e:
            raise e

    def _get_raw_prev_bid(self, historical_ad_df):
        return historical_ad_df['bidding_price'].values[-1]

    def _get_prev_bid(self, historical_ad_df):
        prev_bid = self._get_raw_prev_bid(historical_ad_df)

        if pd.isnull(prev_bid):
            prev_bid = historical_ad_df['minimum_bidding_price'].values[-1]

        return prev_bid

    def _calc_bidding_price_by_rule(self, unit_df, ad_df):
        historical_mask = ad_df['date'] < ad_df['date'].values[-1]
        historical_ad_df = ad_df.loc[historical_mask, :]

        prev_bid = self._get_prev_bid(historical_ad_df)
        bid = prev_bid
        # インプレッションが少なすぎる広告の入札額を強化する
        weekly_impression = np.sum(historical_ad_df['impressions'].values[-7:])
        if weekly_impression < BiddingRuleConfig.THRESHOLD_OF_IMPRESSIONS_WEEKLY:
            bid = prev_bid * BiddingRuleConfig.BIDDING_PRICE_UP_RATIO

        return bid

    def _calc_bidding_price_by_ml(self, unit_df, ad_df, is_provisional_bidding):
        def _calc_origin_bid_by_v(purpose, kpi, v, p, q, C, cvr, cpc, rpc):
            if kpi is KPI.NULL:
                return v / p
            elif purpose is Purpose.CLICK and kpi is KPI.CPC:
                return v / (p + q) + C * q / cpc / (p + q)
            elif purpose is Purpose.CONVERSION and kpi is KPI.CPA:
                return v / (p + q) + C * cvr * q / cpc / (p + q)
            elif purpose is Purpose.SALES and kpi is KPI.ROAS:
                return v / (p + q) + C * rpc * q / cpc / (p + q)
            else:
                raise ValueError

        def _calc_v(purpose, cvr, cpc, rpc):
            if purpose is Purpose.CLICK:
                return 1 / cpc
            elif purpose is Purpose.CONVERSION:
                return cvr / cpc
            else:
                return rpc / cpc

        def _calc_v_provisional(purpose, unit_df, ad_df, ws=28):
            # 通常の価値計算で使う値が揃っていなかった場合の暫定的な価値計算
            monthly_ad_df = ad_df.loc[
                (ad_df['date'] >= self._today - timedelta(days=ws + 1))
                & (ad_df['date'] < self._today), :]

            monthly_unit_df = unit_df.loc[
                (unit_df['date'] >= self._today - timedelta(days=ws + 1))
                & (unit_df['date'] < self._today), :]

            sum_click_last_four_weeks = np.sum(monthly_ad_df["clicks"].values)
            sum_cost_last_four_weeks = np.sum(monthly_ad_df["costs"].values)
            cpc_last_four_weeks = sum_cost_last_four_weeks / sum_click_last_four_weeks

            unit_ewm_sales = monthly_unit_df['sales'].ewm(alpha=0.2).mean().values[-1]
            unit_ewm_conversions = monthly_unit_df['conversions'].ewm(alpha=0.2).mean().values[-1]
            unit_ewm_clicks = monthly_unit_df['clicks'].ewm(alpha=0.2).mean().values[-1]
            unit_ewm_cvr = unit_ewm_conversions / unit_ewm_clicks

            if purpose == Purpose.CONVERSION:
                # cvrが1/クリック数だと仮定。cpcは実績を使用
                v = min(1 / sum_click_last_four_weeks, unit_ewm_cvr) / cpc_last_four_weeks
            elif purpose == Purpose.SALES:
                if unit_ewm_conversions > 0:
                    # cvrが1/クリック数だと仮定。cpcは実績を使用。spaはユニット全体の値を使用。
                    v = (
                        min(1 / sum_click_last_four_weeks, unit_ewm_cvr) * unit_ewm_sales
                        / unit_ewm_conversions / cpc_last_four_weeks
                    )
                else:
                    v = None
            else:
                raise ValueError

            return v, sum_click_last_four_weeks, sum_cost_last_four_weeks, cpc_last_four_weeks

        def _calc_origin_bid(unit_df, ad_df, prev_bid, is_provisional_bidding):
            cvr = ad_df['cvr'].values[-1]
            rpc = ad_df['rpc'].values[-1]
            cpc = ad_df['cpc'].values[-1]
            C = ad_df['C'].values[-1]
            kpi = get_kpi(ad_df['target_kpi'].values[-1])
            purpose = get_purpose(ad_df['purpose'].values[-1])
            p = ad_df['p'].values[-1]
            q = ad_df['q'].values[-1]

            if (q is not None and q == 0) or p == 0:
                raise ValueError("Value of p or q is 0")
            else:
                p = max(1e-16, float(p))
                q = max(1e-16, float(q)) if q is not None else None

            # 広告価値v
            if is_provisional_bidding:
                v, sum_click_last_four_weeks, sum_cost_last_four_weeks, cpc_last_four_weeks = \
                    _calc_v_provisional(purpose, unit_df, ad_df)
            else:
                v = _calc_v(purpose, cvr, cpc, rpc)

            # vをを使用し入札額を計算。vが計算不能だった場合は前日の入札額を継続使用
            bid = _calc_origin_bid_by_v(
                purpose, kpi, v, p, q, C, cvr, cpc, rpc) if v is not None else prev_bid

            return (
                bid,
                v,
                sum_click_last_four_weeks if 'sum_click_last_four_weeks' in locals() else None,
                sum_cost_last_four_weeks if 'sum_cost_last_four_weeks' in locals() else None,
                cpc_last_four_weeks if 'cpc_last_four_weeks' in locals() else None,
            )

        def _bid_upper_limit(*args):
            candidates = [x for x in args if x is not None]

            if len(candidates) == 0:
                return None

            return min(candidates)

        def _get_bid_ratio_range(ad_df) -> Tuple[float, float]:
            if ad_df['target_cost'].values[-1] <= ad_df['base_target_cost'].values[-1]:
                # 予算超過の場合
                return BiddingMLConfig.BIDDING_UB_RATIO_OVER, BiddingMLConfig.BIDDING_LB_RATIO_OVER
            else:
                # 予算過小の場合
                return BiddingMLConfig.BIDDING_UB_RATIO_SHORT, BiddingMLConfig.BIDDING_LB_RATIO_SHORT

        historical_mask = ad_df['date'] < self._today
        historical_ad_df = ad_df.loc[historical_mask, :]
        ub_ratio, lb_ratio = _get_bid_ratio_range(ad_df)
        prev_bid = self._get_prev_bid(historical_ad_df)
        (
            origin_bid,
            ad_value,
            sum_click_last_four_weeks,
            sum_cost_last_four_weeks,
            cpc_last_four_weeks
        ) = _calc_origin_bid(unit_df, ad_df, prev_bid, is_provisional_bidding)
        bid_cpc_limit, unit_period_cpc, ad_ema_weekly_cpc = self._calc_bid_cpc_limit(unit_df, ad_df)

        bid = clip(origin_bid, prev_bid * lb_ratio, _bid_upper_limit(prev_bid * ub_ratio, bid_cpc_limit))

        return (
            bid,
            origin_bid,
            unit_period_cpc,
            ad_ema_weekly_cpc,
            ad_value,
            sum_click_last_four_weeks,
            sum_cost_last_four_weeks,
            cpc_last_four_weeks
        )

    def _clip_bad_performance(self, bid, raw_prev_bid, ad_df):
        unit_weekly_ema_costs = ad_df["unit_weekly_ema_costs"].values[-1]
        unit_ex_observed_C = ad_df["unit_ex_observed_C"].values[-1]
        ad_weekly_ema_costs = ad_df["ad_weekly_ema_costs"].values[-1]
        ad_observed_C_yesterday_in_month = ad_df["ad_observed_C_yesterday_in_month"].values[-1]
        C = ad_df['C'].values[-1]
        mode = ad_df['mode'].values[-1]
        target_cost = ad_df['target_cost'].values[-1]
        base_target_cost = ad_df['base_target_cost'].values[-1]

        if self._today.day != 1 and C is not None:
            if (
                (mode == MODE_KPI and unit_ex_observed_C > C)
                or (mode != MODE_KPI and target_cost < base_target_cost and unit_ex_observed_C > C)
            ):
                if (
                    ad_weekly_ema_costs > unit_weekly_ema_costs * 0.01
                    and ad_observed_C_yesterday_in_month > C
                ):
                    # 広告費用がユニット全体の1%以上かつ、前日のkpiがtarget_kpiよりも悪化している広告は入札額を増加させない
                    # 前日の入札額がNoneの場合、この処理は実質無効
                    return clip(bid, None, raw_prev_bid)

        return bid

    def _calc(self, df, campaign_all_actual_df):
        records = []
        for (advertising_account_id, portfolio_id), tmp_unit_df in df.groupby(
                ['advertising_account_id', 'portfolio_id'], dropna=False):
            if portfolio_id is pd.NA:
                mask = (
                    (campaign_all_actual_df["advertising_account_id"] == advertising_account_id)
                    & (campaign_all_actual_df["portfolio_id"].isnull())
                )
            else:
                mask = (
                    (campaign_all_actual_df["advertising_account_id"] == advertising_account_id)
                    & (campaign_all_actual_df["portfolio_id"] == portfolio_id)
                )
            unit_df = campaign_all_actual_df[mask].groupby(
                ["advertising_account_id", "portfolio_id", "date"], dropna=False
            ).sum(numeric_only=True).sort_values("date").reset_index()
            for (campaign_id, ad_type, ad_id), ad_df in tmp_unit_df.groupby(
                    ['campaign_id', 'ad_type', 'ad_id'], dropna=False):
                # 入札額最適化の実施フラグがFALSEの場合はスキップ
                if ad_df["is_enabled_bidding_auto_adjustment"].values[-1] is False:
                    continue
                round_up_point = int(ad_df['round_up_point'].values[-1])
                raw_prev_bid = self._get_raw_prev_bid(ad_df.loc[ad_df['date'] < self._today, :])
                prev_bid = self._get_prev_bid(ad_df.loc[ad_df['date'] < self._today, :])
                origin_bid, unit_period_cpc, ad_ema_weekly_cpc, has_exception = \
                    None, None, None, False

                try:
                    is_by_ml = self._is_by_ml(ad_df)
                    is_provisional_bidding = self._is_provisional_bidding(ad_df)
                    ad_value = None
                    sum_click_last_four_weeks = None
                    sum_cost_last_four_weeks = None
                    cpc_last_four_weeks = None

                    if is_by_ml:
                        (
                            bid,
                            origin_bid,
                            unit_period_cpc,
                            ad_ema_weekly_cpc,
                            ad_value,
                            sum_click_last_four_weeks,
                            sum_cost_last_four_weeks,
                            cpc_last_four_weeks,
                        ) = self._calc_bidding_price_by_ml(
                            unit_df,
                            ad_df,
                            is_provisional_bidding
                        )
                    else:
                        bid = self._calc_bidding_price_by_rule(unit_df, ad_df)

                    # clip処理
                    minimum_bidding_price = ad_df['minimum_bidding_price'].values[-1]
                    maximum_bidding_price = ad_df['maximum_bidding_price'].values[-1]

                    # 成績が悪い場合のクリッピング
                    bid = self._clip_bad_performance(bid, raw_prev_bid, ad_df)

                    # 異常値判定
                    self._check_abnormal(bid)

                    # 最低入札額、最高入札額の範囲でクリッピング
                    bid = clip(bid, minimum_bidding_price, maximum_bidding_price)

                    ceil_point = 10 ** (round_up_point - 1)
                    bid = math.ceil(bid * ceil_point) / ceil_point
                except Exception as e:
                    logger.error(
                        "Exception occurred in calc "
                        f"(advertising_account_id: {advertising_account_id}, portfolio_id: {portfolio_id},"
                        f"campaign_id: {campaign_id}, ad_type: {ad_type}, ad_id: {ad_id}) "
                        f"{str(e)}"
                        f"[traceback]{traceback.format_exc()}"
                    )
                    bid = prev_bid
                    has_exception = True

                record = {
                    'advertising_account_id': advertising_account_id,
                    'portfolio_id': portfolio_id,
                    'campaign_id': campaign_id,
                    'ad_type': ad_type,
                    'ad_id': ad_id,
                    'date': self._today,
                    'is_ml_applied': is_by_ml,
                    'is_provisional_bidding': is_provisional_bidding,
                    'bidding_price': bid,
                    'origin_bidding_price': origin_bid,
                    'unit_cpc': unit_period_cpc,
                    'ad_ema_weekly_cpc': ad_ema_weekly_cpc,
                    'has_exception': has_exception,
                    'ad_value': ad_value,
                    'sum_click_last_four_weeks': sum_click_last_four_weeks,
                    'sum_cost_last_four_weeks': sum_cost_last_four_weeks,
                    'cpc_last_four_weeks': cpc_last_four_weeks,
                }
                records.append(record)

        return records

    def _empty_result(self):
        return pd.DataFrame(columns=list(OUTPUT_DTYPES.keys()))

    def calc(self, df, campaign_all_actual_df, bidding_algorithm="") -> pd.DataFrame:
        if len(df) == 0 or len(campaign_all_actual_df) == 0:
            df = self._empty_result()
        else:
            records = self._calc(df, campaign_all_actual_df)
            df = pd.DataFrame.from_records(records)
            if len(df) > 0:
                df["bidding_algorithm"] = bidding_algorithm
                df = df.astype(OUTPUT_DTYPES)
            else:
                df = self._empty_result()

        logger.info(f"output records: {len(df)}")
        return df

    def _calc_bid_cpc_limit(self, unit_df, ad_df, ws=14):
        end = self._today
        start = end - timedelta(days=ws + 1)
        unit_historical_mask = (unit_df['date'] >= start) & (unit_df['date'] < end)
        ad_historical_mask = (ad_df['date'] >= start) & (ad_df['date'] < end)

        historical_unit_df = unit_df.loc[unit_historical_mask, :]
        historical_ad_df = ad_df.loc[ad_historical_mask, :]

        if len(historical_unit_df) > 0:
            agg_unit = historical_unit_df.groupby('date')[['clicks', 'costs']].sum().reset_index()
            agg_unit = agg_unit.sort_values('date')
            unit_period_cpc = weighted_cpc(
                ws,
                np.array(agg_unit['clicks'].values),
                np.array(agg_unit['costs'].values)
            )
        else:
            unit_period_cpc = 0

        if len(historical_ad_df) > 0:
            historical_ad_df = historical_ad_df.sort_values('date').reset_index()
            ad_ema_period_cpc = weighted_cpc(
                ws,
                np.array(historical_ad_df['clicks'].values),
                np.array(historical_ad_df['costs'].values)
            )
        else:
            ad_ema_period_cpc = 0

        # 広告の1週間のCPCが0より大きい場合
        if ad_ema_period_cpc > 0:
            return (
                max(ad_ema_period_cpc * CPC_LIMIT_RATE, unit_period_cpc * CPC_LIMIT_RATE),
                unit_period_cpc,
                ad_ema_period_cpc
            )
        # 広告の1週間のCPCが0より大きい場合
        elif unit_period_cpc > 0:
            return (
                unit_period_cpc * CPC_LIMIT_RATE,
                unit_period_cpc,
                ad_ema_period_cpc
            )
        else:
            return (None, None, ad_ema_period_cpc)
