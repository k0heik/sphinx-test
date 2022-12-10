import os
from collections import ChainMap

import pandas as pd
from common_module.logger_util import get_custom_logger
from common_module.bigquery_util import BigQueryService

from spai.service.pid.config import ML_LOOKUP_DAYS

from module import args, prepare_df


logger = get_custom_logger()

_GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
_DATASET_NAME = os.environ["DATASET_NAME"]
_COMMERCE_FLOW_DATASET_NAME = os.environ["COMMERCE_FLOW_DATASET_NAME"]
_USE_CACHE = os.environ.get("USE_BQ_CACHE", "no") == "yes"

_bq = BigQueryService(
    project_id=_GCP_PROJECT_ID,
    dataset_name=_DATASET_NAME,
)


def _extract(sqlfile, extra_date_columns=[], add_params={}):
    logger.info(sqlfile)
    df = _bq.extract_to_df(
        os.path.join(os.path.dirname(__file__), "../sql"),
        sqlfile,
        ChainMap(
            {
                "today": args.Event.today.strftime("%Y-%m-%d"),
                "project": _GCP_PROJECT_ID,
                "dataset": _DATASET_NAME,
                "commerce_flow_dataset": _COMMERCE_FLOW_DATASET_NAME,
                "advertising_account_id": args.Event.advertising_account_id,
                "portfolio_id": args.Event.portfolio_id,
            },
            add_params,
        ),
        use_cache=_USE_CACHE,
    )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    if "portfolio_id" in df.columns:
        df["portfolio_id"] = df["portfolio_id"].astype("Int64")

    for col in extra_date_columns:
        df[col] = pd.to_datetime(df[col])

    return df


def basic():
    unit_info_df = _extract("unit_info.tpl.sql", extra_date_columns=["start"])
    unit_info_df = prepare_df.add_unit_info_setting_columns(unit_info_df)
    return (
        unit_info_df,
        _extract("campaign_info.tpl.sql"),
        _extract("campaign_all_actual.tpl.sql"),
        _extract("ad_info.tpl.sql"),
        _extract("ad_target_actual.tpl.sql"),
        _extract("daily_budget_boost_coefficient.tpl.sql"),
    )


def campaign_placement():
    return _extract("campaign_placement.tpl.sql")


def keyword_queries():
    return _extract("keyword_queries.tpl.sql")


def lastday_ml_result_ad():
    return _extract("lastday_ml_result_ad.tpl.sql")


def lastday_ml_result_campaign():
    return _extract("lastday_ml_result_campaign.tpl.sql")


def lastday_ml_result_unit():
    return _extract("lastday_ml_result_unit.tpl.sql")


def ml_applied_history():
    return _extract(
        "ml_applied_history.tpl.sql", add_params={"ml_lookup_days": ML_LOOKUP_DAYS}
    )


def ad_input_json():
    df = _extract("ad_input_json.tpl.sql")

    df["sales"] = df["sales"].astype(float)
    df["costs"] = df["costs"].astype(float)
    df["bidding_price"] = df["bidding_price"].astype(float)
    df["daily_budget"] = df["daily_budget"].astype(float)

    return prepare_df.add_unit_key(df)
