import os
import pandas as pd

from common_module.logger_util import get_custom_logger
from common_module.aws_util import write_json_to_s3

from module import config, args, prepare_df, libs

from .format import OutputFormat
from .schema import OutputRootSchema


logger = get_custom_logger()


_INPUT_DF_COLS = [
    "advertising_account_id",
    "portfolio_id",
    "campaign_id",
    "ad_type",
    "ad_id",
    "is_paused",
    "daily_budget",
    "bidding_price",
    "last_bidding_price",
    "last_daily_budget",
    "is_enabled_daily_budget_auto_adjustment",
    "is_enabled_bidding_auto_adjustment",
]


def s3_output_path():
    date_prefix, file_prefix = libs.s3_output_prefix()

    return os.path.join(
        os.environ["OUTPUT_JSON_PREFIX"], f"{date_prefix}/{file_prefix}.json"
    )


def _prepare_df(
    ad_input_json_df, bid_optimiser_df, cap_daily_budget_df, target_pause_df
):
    df = ad_input_json_df.sort_values(
        config.CAMPAIGN_KEY + config.AD_KEY + config.DATE_KEY
    )
    df = (
        df.groupby(config.AD_KEY)
        .last()
        .reset_index()
        .rename(
            columns={
                "daily_budget": "last_daily_budget",
                "bidding_price": "last_bidding_price",
            }
        )
    )

    prepare_df.add_unit_key(df)

    if bid_optimiser_df is None:
        df["bidding_price"] = df["last_bidding_price"]
    else:
        df = pd.merge(
            df,
            bid_optimiser_df[config.AD_KEY + ["bidding_price"]],
            how="left",
            on=config.AD_KEY,
        )

    if cap_daily_budget_df is None:
        df["daily_budget_upper"] = df["last_daily_budget"]
    else:
        df = pd.merge(
            df,
            cap_daily_budget_df[config.CAMPAIGN_KEY + ["daily_budget_upper"]],
            how="left",
            on=config.CAMPAIGN_KEY,
        )

    if target_pause_df is None:
        df["is_target_pause"] = False
    else:
        df = pd.merge(
            df,
            target_pause_df[config.AD_KEY + ["is_target_pause"]],
            how="left",
            on=config.AD_KEY,
        )

    df = df.rename(
        columns={
            "daily_budget_upper": "daily_budget",
            "is_target_pause": "is_paused",
        }
    )

    df["bidding_price"].fillna(df["last_bidding_price"], inplace=True)
    df["daily_budget"].fillna(df["last_daily_budget"], inplace=True)

    return df[_INPUT_DF_COLS]


def exec(ad_input_json_df, bid_optimiser_df, cap_daily_budget_df, target_pause_df):
    advertising_account_id = args.Event.advertising_account_id
    portfolio_id = args.Event.portfolio_id

    write_json_to_s3(
        OutputRootSchema().dumps(
            OutputFormat(
                advertising_account_id,
                portfolio_id,
                _prepare_df(
                    ad_input_json_df,
                    bid_optimiser_df,
                    cap_daily_budget_df,
                    target_pause_df,
                ),
            ).get_formatted()
        ),
        os.environ["OUTPUT_JSON_BUCKET"],
        s3_output_path(),
    )
