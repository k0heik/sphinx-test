import datetime
import pandas as pd

from spai.service.cap.service import CAPCalculationService
from spai.service.cap.preprocess import CAPPreprocessor, INPUT_COLUMNS, INPUT_DAILY_COLUMNS
from spai.service.cap.calculator import CAPCalculator

from module import config, args, prepare_df


_CAP_DATA_DAYS = 28

_INPUT_DF_COLS = INPUT_COLUMNS
_INPUT_DAILY_DF_COLS = INPUT_DAILY_COLUMNS


def _prepare_df(
    _ad_input_json_df, lastday_cap_weight_df, lastday_ml_result_unit_df,
    target_campaign_df, target_unit_df, campaign_info_df, daily_budget_boost_coefficient_df,
):
    today = args.Event.today
    yesterday = today - datetime.timedelta(days=1)

    df = _ad_input_json_df.groupby(
        config.UNIT_KEY + config.CAMPAIGN_KEY, dropna=False
    ).last().reset_index()[
        config.UNIT_KEY + config.CAMPAIGN_KEY
        + ["daily_budget", "minimum_daily_budget", "maximum_daily_budget"]
    ]

    df["date"] = args.Event.today

    df["yesterday_target_cost"] = (
        lastday_ml_result_unit_df["target_cost"].values[0]
        if len(lastday_ml_result_unit_df) > 0 else None
    )
    monthly_coefficient_df = prepare_df.shape_daily_budget_boost_coefficient(
        daily_budget_boost_coefficient_df,
        today - datetime.timedelta(days=1),
        today
    )
    monthly_coefficient_df["yesterday_coefficient"] = monthly_coefficient_df["coefficient"].shift(1)
    monthly_coefficient_df = monthly_coefficient_df.rename(
        columns={
            "coefficient": "today_coefficient",
        }
    )
    today_target_unit_df = target_unit_df.copy()
    today_target_unit_df["date"] = today
    today_target_unit_df = pd.merge(today_target_unit_df, monthly_coefficient_df, how="left", on=["date"])
    df = pd.merge(df, today_target_unit_df, how="left", on=config.UNIT_KEY, suffixes=["", "_y"])
    df = pd.merge(
        df, campaign_info_df, how="left", on=config.UNIT_KEY + config.CAMPAIGN_KEY)
    df = pd.merge(
        df, target_campaign_df[target_campaign_df["date"] == yesterday],
        how="left", on=config.CAMPAIGN_KEY, suffixes=["", "_y"])

    daily_df = target_campaign_df.copy()
    daily_df = daily_df[
        (daily_df["date"] < today)
        & (
            daily_df["date"]
            >= today - datetime.timedelta(days=_CAP_DATA_DAYS)
        )
    ]
    daily_df = pd.merge(
        daily_df, lastday_cap_weight_df, how="left", on=config.CAMPAIGN_KEY + config.DATE_KEY
    )

    return (
        df[_INPUT_DF_COLS],
        daily_df.rename(
            columns={
                "unit_cumsum_costs": "cumsum_costs",
                "cap_daily_budget_weight": "weight",
            }
        )[_INPUT_DAILY_DF_COLS]
    )


def exec(
    ad_input_json_df,
    lastday_cap_weight_df,
    lastday_ml_result_unit_df,
    target_campaign_df,
    target_unit_df,
    campaign_info_df,
    campaign_all_actual_df,
    daily_budget_boost_coefficient_df,
):
    return CAPCalculationService(
        preprocessor=CAPPreprocessor(args.Event.today),
        calculator=CAPCalculator(args.Event.today),
    ).calc(
        *_prepare_df(
            ad_input_json_df, lastday_cap_weight_df, lastday_ml_result_unit_df,
            target_campaign_df, target_unit_df, campaign_info_df, daily_budget_boost_coefficient_df,
        ),
        prepare_df.add_unit_key(campaign_all_actual_df),
    )
