import traceback
import datetime
import pandas as pd

from spai.service.pid.service import PIDCalculationService
from spai.service.pid.preprocess import PIDPreprocessor
from spai.service.pid.calculator import PIDCalculator
from spai.service.pid.config import ML_LOOKUP_DAYS
from spai.utils.kpi.kpi import safe_div

from common_module.logger_util import get_custom_logger

from module import config, args


logger = get_custom_logger()


_PID_DATA_DAYS = 31
_INPUT_DF_COLS = [
    "advertising_account_id",
    "portfolio_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "date",
    "bidding_price",
    "impressions",
    "clicks",
    "costs",
    "conversions",
    "sales",
    "optimization_costs",
    "optimization_priority_mode_type",
    "optimization_purpose",
    "optimization_purpose_value",
    "is_enabled_bidding_auto_adjustment",
    "ctr",
    "cvr",
    "rpc",
    "cpc",
    "p",
    "q",
    "p_kp",
    "p_ki",
    "p_kd",
    "p_error",
    "p_sum_error",
    "q_kp",
    "q_ki",
    "q_kd",
    "q_error",
    "q_sum_error",
    "weekly_clicks",
    "monthly_conversions",
    "monthly_sales",
    "sum_costs",
    "not_ml_applied_days",
    "target_kpi",
    "target_kpi_value",
    "purpose",
    "mode",
    "remaining_days",
    "target_cost",
    "base_target_cost",
    "yesterday_target_kpi",
    "unit_ex_observed_C",
]


def _calc_sum_actual(df, name, freq):
    sum_df = (
        df.sort_values(config.AD_KEY + config.DATE_KEY)[
            config.AD_KEY + config.DATE_KEY + config.ACTUAL_COLS
        ]
        .groupby(config.AD_KEY)
        .rolling(freq, on="date", min_periods=1)
        .sum()
    )

    return pd.merge(
        df,
        sum_df,
        how="left",
        on=config.AD_KEY + config.DATE_KEY,
        suffixes=["", f"_{name}"],
    )


def _not_ml_applied_days(yesterday, is_lastday_ml_applied, ml_applied_history_df):
    if is_lastday_ml_applied:
        return 0
    elif len(ml_applied_history_df) == 0:
        return ML_LOOKUP_DAYS
    else:
        return (yesterday - ml_applied_history_df["date"].max()) / datetime.timedelta(
            days=1
        )


def _prepare_df(
    _df,
    target_unit_df,
    lastday_ml_result_unit_df,
    is_lastday_ml_applied,
    ml_applied_history_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
):
    today = args.Event.today
    yesterday = today - datetime.timedelta(days=1)

    pid_reuslt_columns = [
        "date",
        "p",
        "q",
        "p_kp",
        "p_ki",
        "p_kd",
        "p_error",
        "p_sum_error",
        "q_kp",
        "q_ki",
        "q_kd",
        "q_error",
        "q_sum_error",
        "target_kpi",
    ]

    if len(lastday_ml_result_unit_df) == 0:
        lastday_ml_result_unit_df = pd.DataFrame(columns=pid_reuslt_columns)

    df = _df[
        (_df["date"] < today)
        & (_df["date"] >= today - datetime.timedelta(days=_PID_DATA_DAYS))
    ]

    # PID履歴をマージ
    df = pd.merge(df, lastday_ml_result_unit_df.rename(columns={
        "target_kpi": "yesterday_target_kpi",
        "target_cost": "yesterday_target_cost",
    }), how="left", on=config.DATE_KEY)

    # KPI計算
    df["ctr"] = safe_div(df["clicks"], df["impressions"])
    df["cpc"] = safe_div(df["costs"], df["clicks"])
    df["cvr"] = safe_div(df["conversions"], df["clicks"])
    df["rpc"] = safe_div(df["sales"], df["clicks"])

    # weekly, monthly の合計値を算出してマージ
    df = _calc_sum_actual(df, "weekly", "7D")
    df = _calc_sum_actual(df, "monthly", "28D")

    # ML連続非適用日数を算出
    df["not_ml_applied_days"] = _not_ml_applied_days(
        yesterday, is_lastday_ml_applied, ml_applied_history_df
    )

    # KPI予測結果のマージ
    prediction_df = pd.merge(
        cpc_prediction_df, cvr_prediction_df,
        how="left", on=config.AD_KEY + ["date"])
    prediction_df = pd.merge(
        prediction_df, spa_prediction_df,
        how="left", on=config.AD_KEY + ["date"])

    today_df = df.groupby(config.AD_KEY).last().reset_index()

    today_df["date"] = today

    today_df = pd.merge(
        today_df, prediction_df,
        how="left", on=config.AD_KEY + ["date"], suffixes=["_x", ""]
    )

    today_df["ctr"] = None
    today_df["rpc"] = today_df["cvr"] * today_df["spa"]

    df = (
        pd.concat([df, today_df[df.columns]])
        .reset_index(drop=True)
        .sort_values(config.CAMPAIGN_KEY + config.AD_KEY + config.DATE_KEY)
        .reset_index(drop=True)
    )

    # unit単位の情報をマージ
    today_target_unit_df = target_unit_df.copy()
    today_target_unit_df["date"] = today
    df = pd.merge(
        df, today_target_unit_df,
        how="left", on=config.UNIT_KEY + config.DATE_KEY,
        suffixes=["", "_y"],
    )

    return df.rename(
        columns={
            "unit_sum_costs": "sum_costs",
            "clicks_weekly": "weekly_clicks",
            "conversions_monthly": "monthly_conversions",
            "sales_monthly": "monthly_sales",
            "ideal_target_cost": "base_target_cost",
        }
    )[_INPUT_DF_COLS]


def exec(
    df,
    target_unit_df,
    pid_weight_history_df,
    is_lastday_ml_applied,
    ml_applied_history_df,
    campaign_all_actual_df,
    cpc_prediction_df,
    cvr_prediction_df,
    spa_prediction_df,
):

    try:
        return PIDCalculationService(
            preprocessor=PIDPreprocessor(),
            calculator=PIDCalculator(args.Event.today),
        ).calc(
            _prepare_df(
                df,
                target_unit_df,
                pid_weight_history_df,
                is_lastday_ml_applied,
                ml_applied_history_df,
                cpc_prediction_df,
                cvr_prediction_df,
                spa_prediction_df,
            ),
            campaign_all_actual_df,
        )
    except Exception as e:
        logger.error(
            "pid_controller raises exception."
            f" advertising_account_id: {args.Event.advertising_account_id},"
            f" portfolio_id: {args.Event.portfolio_id}\n"
            f"\n[error]{e}\n[traceback]{traceback.format_exc()}"
        )
        return None
