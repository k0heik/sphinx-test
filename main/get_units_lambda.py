import os
import pandas as pd

from common_module.logger_util import get_custom_logger
from common_module.target_unit_util import get_target_unit_df
from common_module.system_util import get_target_date

from module import get_units_extract


logger = get_custom_logger()

_TARGET_UNIT_DATASOURCE_BQ = "BQ"
_TARGET_UNIT_DATASOURCE_CSV = "CSV"


def get_units(date: str):
    TARGET_UNIT_DATASOURCE = os.environ["TARGET_UNIT_DATASOURCE"]

    if TARGET_UNIT_DATASOURCE == _TARGET_UNIT_DATASOURCE_BQ:
        today = get_target_date(date)
        df = get_units_extract.get_all_input_units(today)
    elif TARGET_UNIT_DATASOURCE == _TARGET_UNIT_DATASOURCE_CSV:
        df = get_target_unit_df()
    else:
        raise ValueError(f"TARGET_UNIT_DATASOURCE={TARGET_UNIT_DATASOURCE} is invalid.")

    units = []
    for advertising_account_id, portfolio_id in zip(
        df["advertising_account_id"], df["portfolio_id"]
    ):
        units.append(
            {
                "date": date,
                "advertising_account_id": advertising_account_id,
                "portfolio_id": None if portfolio_id is pd.NA else str(portfolio_id),
            }
        )

    return units


def lambda_handler(event, context):
    logger.info("start lambda")
    logger.info(event)
    date = event["date"]
    units = get_units(date)
    logger.info(f"units: {units}")
    logger.info("success lambda")
    return units
