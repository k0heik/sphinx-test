import os

import pandas as pd
import numpy as np
from common_module.aws_util import write_df_to_s3

from spai.service.pid.config import OUTPUT_COLUMNS as PID_OUTPUT_COLUMNS
from spai.service.bid.config import OUTPUT_DTYPES as BID_OUTPUT_DTYPES
from spai.service.cap.config import OUTPUT_COLUMNS as CAP_OUTPUT_COLUMNS
from module.target_pause import OUTPUT_COLUMNS as TARGET_PAUSE_OUTPUT_COLUMNS
from module import libs, config, args, prepare_df

from .config import (
    OUTPUT_CSV_UNIT_COLUMNS,
    OUTPUT_CSV_CAMPAIGN_COLUMNS,
    OUTPUT_CSV_AD_COLUMNS,
)

BID_OUTPUT_COLUMNS = list(BID_OUTPUT_DTYPES.keys())

CSV_DATA_TYPE_UNIT = "unit"
CSV_DATA_TYPE_CAMPAIGN = "campaign"
CSV_DATA_TYPE_AD = "ad"


def _group_and_merge(base_df, merge_df_maps, key):
    df = base_df.groupby(key, dropna=False).last().reset_index()

    for m_df, suffixes in merge_df_maps:
        df = pd.merge(
            df,
            m_df.groupby(key, dropna=False).last(),
            how="left",
            on=key,
            suffixes=suffixes,
        )

    return df


def _none_to_zero(df, col_list):
    if df is None:
        return pd.DataFrame(columns=col_list)

    return df


def _prepare_df_unit(
    target_unit_df,
    is_lastday_ml_applied,
    bid_optimiser_df,
    target_pause_df,
    cap_daily_budget_df,
    pid_controller_df,
    lastday_ml_result_unit_df,
):
    df = _group_and_merge(
        target_unit_df,
        [
            (_none_to_zero(pid_controller_df, PID_OUTPUT_COLUMNS), ["", "_pid"]),
            (_none_to_zero(cap_daily_budget_df, CAP_OUTPUT_COLUMNS), ["", "_cap"]),
            (_none_to_zero(bid_optimiser_df, BID_OUTPUT_COLUMNS), ["", "_bid"]),
            (prepare_df.add_unit_key(lastday_ml_result_unit_df), ["", "_yesterday"]),
        ],
        key=config.UNIT_KEY,
    )

    df["is_ml_target"] = True
    df["is_ml_enabled"] = True
    df["is_target_pause_judged"] = target_pause_df is not None
    df["is_lastday_ml_applied"] = is_lastday_ml_applied
    df["remain_budget"] = df["optimization_costs"] - df["used_costs"]
    df["is_pid_failed"] = pid_controller_df is None
    df["C"] = np.where(
        df["target_kpi"] == "ROAS",
        1 / df["target_kpi_value"],
        df["target_kpi_value"]
    )

    df["date"] = args.Event.today

    return df.rename(
        columns={
            "unit_cpc": "unit_period_cpc",
            "optimization_purpose_value": "target_kpi_value_origin",
            "coefficient": "daily_budget_boost_coefficient_today",
            "total_coefficient": "daily_budget_boost_coefficient_total",
            "used_coefficient": "daily_budget_boost_coefficient_used",
            "remaining_coefficient": "daily_budget_boost_coefficient_remaining",
            "obs_kpi": "obs_kpi_for_pid",
            "target_cost_yesterday": "yesterday_target_cost",
            "target_kpi_yesterday": "yesterday_target_kpi",
        }
    )[OUTPUT_CSV_UNIT_COLUMNS]


def _prepare_df_campaign(
    target_campaign_df,
    cap_daily_budget_df,
    ad_input_json_df,
):
    df = _group_and_merge(
        target_campaign_df, [
            (_none_to_zero(cap_daily_budget_df, CAP_OUTPUT_COLUMNS), ["", "_cap"]),
            (ad_input_json_df, ["", "_input"]),
        ],
        key=config.CAMPAIGN_KEY,
    )

    df["date"] = args.Event.today

    return df.rename(
        columns={
            "weight": "cap_daily_budget_weight",
            "daily_budget_upper": "cap_daily_budget",
            "daily_budget": "input_daily_budget",
        }
    )[OUTPUT_CSV_CAMPAIGN_COLUMNS]


def _prepare_df_ad(
    target_ad_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    target_pause_df,
    bid_optimiser_df,
    ad_input_json_df,
):

    df = _group_and_merge(
        target_ad_df,
        [
            (_none_to_zero(bid_optimiser_df, BID_OUTPUT_COLUMNS), ["", "_bid"]),
            (cpc_prediction_df, ["", "_cpc"]),
            (cvr_prediction_df, ["", "_cvr"]),
            (spa_prediction_df, ["", "_spa"]),
            (_none_to_zero(target_pause_df, TARGET_PAUSE_OUTPUT_COLUMNS), ["", "_tp"]),
            (ad_input_json_df, ["", "_input"]),
        ],
        key=config.AD_KEY,
    )

    df["date"] = args.Event.today

    return df.rename(
        columns={
            "cpc": "predicted_cpc",
            "cvr": "predicted_cvr",
            "spa": "predicted_spa",
            "bidding_price": "x_bidding_price",
            "bidding_price_input": "input_bidding_price",
            "bidding_price_bid": "bidding_price",
            "has_exception": "has_bidding_price_exception",
            "ad_ema_weekly_cpc": "ad_period_cpc",
            "is_ml_applied": "is_ml_bidding",
            "is_provisional_bidding": "is_ml_provisional_bidding",
        }
    )[OUTPUT_CSV_AD_COLUMNS]


def _prepare_df(
    target_ad_df,
    target_campaign_df,
    target_unit_df,
    is_lastday_ml_applied,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    bid_optimiser_df,
    target_pause_df,
    cap_daily_budget_df,
    pid_controller_df,
    ad_input_json_df,
    lastday_ml_result_unit_df,
):
    unit_df = _prepare_df_unit(
        target_unit_df,
        is_lastday_ml_applied,
        bid_optimiser_df,
        target_pause_df,
        cap_daily_budget_df,
        pid_controller_df,
        lastday_ml_result_unit_df,
    )

    campaign_df = _prepare_df_campaign(
        target_campaign_df,
        cap_daily_budget_df,
        ad_input_json_df,
    )

    ad_df = _prepare_df_ad(
        target_ad_df,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        target_pause_df,
        bid_optimiser_df,
        ad_input_json_df,
    )

    return unit_df, campaign_df, ad_df


def s3_output_path(data_type):
    date_prefix, file_prefix = libs.s3_output_prefix()

    return os.path.join(
        os.environ["OUTPUT_CSV_PREFIX"],
        f"{date_prefix}/{data_type}/{file_prefix}_{data_type}.csv",
    )


def exec(
    target_ad_df,
    target_campaign_df,
    target_unit_df,
    is_lastday_ml_applied,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
    bid_optimiser_df,
    target_pause_df,
    cap_daily_budget_df,
    pid_controller_df,
    ad_input_json_df,
    lastday_ml_result_unit_df,
):
    OUTPUT_CSV_BUCKET = os.environ["OUTPUT_CSV_BUCKET"]

    ml_result_unit_df, ml_result_campaign_df, ml_result_ad_df = _prepare_df(
        target_ad_df,
        target_campaign_df,
        target_unit_df,
        is_lastday_ml_applied,
        cpc_prediction_df,
        cvr_prediction_df,
        spa_prediction_df,
        bid_optimiser_df,
        target_pause_df,
        cap_daily_budget_df,
        pid_controller_df,
        ad_input_json_df,
        lastday_ml_result_unit_df,
    )

    write_df_to_s3(
        ml_result_unit_df, OUTPUT_CSV_BUCKET, s3_output_path(CSV_DATA_TYPE_UNIT))
    write_df_to_s3(
        ml_result_campaign_df, OUTPUT_CSV_BUCKET, s3_output_path(CSV_DATA_TYPE_CAMPAIGN))
    write_df_to_s3(
        ml_result_ad_df, OUTPUT_CSV_BUCKET, s3_output_path(CSV_DATA_TYPE_AD))
