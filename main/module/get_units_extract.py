import os
from collections import ChainMap

from common_module.logger_util import get_custom_logger
from common_module.bigquery_util import BigQueryService


logger = get_custom_logger()

_GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
_DATASET_NAME = os.environ["DATASET_NAME"]
_USE_CACHE = os.environ.get("USE_BQ_CACHE", "no") == "yes"

_bq = BigQueryService(
    project_id=_GCP_PROJECT_ID,
    dataset_name=_DATASET_NAME,
)


def _extract(sqlfile, add_params={}):
    logger.info(sqlfile)
    df = _bq.extract_to_df(
        os.path.join(os.path.dirname(__file__), "../sql"),
        sqlfile,
        ChainMap(
            {
                "project": _GCP_PROJECT_ID,
                "dataset": _DATASET_NAME,
            },
            add_params,
        ),
        use_cache=_USE_CACHE,
    )

    if "portfolio_id" in df.columns:
        df["portfolio_id"] = df["portfolio_id"].astype("Int64")

    return df


def get_all_input_units(today):
    return _extract("get_all_input_units.tpl.sql", add_params={"today": today})
