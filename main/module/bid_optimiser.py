import pandas as pd
import numpy as np

from spai.service.bid.service import BIDCalculationService
from spai.service.bid.preprocess import BIDPreprocessor
from spai.service.bid.calculator import BIDCalculator

from common_module.logger_util import get_custom_logger

from module import config, args, prepare_df


logger = get_custom_logger()


_BIDDING_ALGORITHM = "SoTA"
_INPUT_DF_COLS = [
    "advertising_account_id",
    "portfolio_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "date",
    "is_enabled_bidding_auto_adjustment",
    "bidding_price",
    "minimum_bidding_price",
    "maximum_bidding_price",
    "impressions",
    "clicks",
    "costs",
    "conversions",
    "sales",
    "cvr",
    "rpc",
    "cpc",
    "optimization_costs",
    "purpose",
    "mode",
    "target_kpi",
    "target_kpi_value",
    "p",
    "q",
    "round_up_point",
    "remaining_days",
    "target_cost",
    "base_target_cost",
    "unit_ex_observed_C",
    "unit_weekly_ema_costs",
    "ad_weekly_ema_costs",
    "ad_observed_C_yesterday_in_month",
]


def _merge_predictions(today_df, prediction_df, name):
    df = pd.merge(
        today_df,
        prediction_df[config.AD_KEY + [name]],
        how="left",
        on=config.AD_KEY,
        suffixes=["_x", ""],
    )

    return df.drop(columns=[f"{name}_x"])


def _prepare_df(
    target_unit_df,
    target_ad_df,
    ad_input_json_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    pid_controller_df,
):
    today = args.Event.today
    df = pd.merge(
        ad_input_json_df, target_ad_df,
        how="left", on=config.AD_KEY + config.DATE_KEY, suffixes=["", "_target"])
    df = pd.merge(
        df,
        pid_controller_df[list(set(pid_controller_df.columns) - set(["date"]))],
        how="left", on=config.UNIT_KEY, suffixes=["", "_pid"])

    df["cvr"] = None
    df["rpc"] = None
    df["cpc"] = None
    df["spa"] = None

    today_df = df.groupby(config.AD_KEY).last().reset_index()

    today_df["date"] = today
    today_df = _merge_predictions(today_df, cpc_prediction_df, "cpc")
    today_df = _merge_predictions(today_df, cvr_prediction_df, "cvr")
    today_df = _merge_predictions(today_df, spa_prediction_df, "spa")
    today_df["rpc"] = np.where(
        today_df["spa"].isnull() | today_df["cvr"].isnull(),
        None,
        today_df["spa"] * today_df["cvr"],
    )

    df = (
        pd.concat([df, today_df])
        .reset_index(drop=True)
        .sort_values(config.CAMPAIGN_KEY + config.AD_KEY + config.DATE_KEY)
        .reset_index(drop=True)
    )

    today_target_unit_df = target_unit_df.copy()
    today_target_unit_df["date"] = today
    df = pd.merge(
        df, today_target_unit_df,
        how="left", on=config.UNIT_KEY + config.DATE_KEY,
        suffixes=["", "_y"],
    )

    df["cvr"] = df["cvr"].astype(float)
    df["cpc"] = df["cpc"].astype(float)
    df["spa"] = df["spa"].astype(float)

    df = df.sort_values(config.UNIT_KEY + config.CAMPAIGN_KEY + config.AD_KEY + config.DATE_KEY)

    return df.rename(
        columns={
            "unit_sum_costs": "sum_costs",
        }
    )[_INPUT_DF_COLS]


def exec(
    target_unit_df,
    target_ad_df,
    ad_input_json_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    pid_controller_df,
    campaign_all_actual_df,
):
    if pid_controller_df is None:
        logger.error(
            "Skip bid_optimiser process, pid_controller result is None."
            f" advertising_account_id: {args.Event.advertising_account_id},"
            f" portfolio_id: {args.Event.portfolio_id}"
        )
        return None
    elif all(pid_controller_df["is_skip_pid_calc_state"]):
        logger.warning(
            "Skip bid_optimiser process, pid_controller calc is skipped."
            f" advertising_account_id: {args.Event.advertising_account_id},"
            f" portfolio_id: {args.Event.portfolio_id}"
        )
        return None
    elif all(pid_controller_df["valid_ads_num"] == 0):
        logger.warning(
            "Skip bid_optimiser process, valid_ads_num is zero and pid_controller calc is skipped."
            f" advertising_account_id: {args.Event.advertising_account_id},"
            f" portfolio_id: {args.Event.portfolio_id}"
        )
        return None
    else:
        return BIDCalculationService(
            preprocessor=BIDPreprocessor(),
            calculator=BIDCalculator(args.Event.today),
        ).calc(
            df=_prepare_df(
                target_unit_df,
                target_ad_df,
                ad_input_json_df,
                cpc_prediction_df,
                cvr_prediction_df,
                spa_prediction_df,
                pid_controller_df,
            ),
            campaign_all_actual_df=prepare_df.add_unit_key(campaign_all_actual_df),
            bidding_algorithm=_BIDDING_ALGORITHM,
            advertising_account_id=args.Event.advertising_account_id,
            portfolio_id=args.Event.portfolio_id,
        )
