import os

import pandas as pd

from common_module.logger_util import get_custom_logger
from common_module.system_util import get_target_date
from common_module.aws_util import load_file_list, write_df_to_s3, read_from_s3
from common_module.bigquery_util import BigQueryService
from common_module.bigquery_util.funcs import format_to_bq_schema


logger = get_custom_logger()


_TABLE_NAME_MAP = {
    "unit": ("ml_result_unit", "ml_log_unit"),
    "campaign": ("ml_result_campaign", "ml_log_campaign"),
    "ad": ("ml_result_ad", "ml_log_ad"),
}


def _log_table_name(log_table_name_prefix, y, m, d):
    return f"{log_table_name_prefix}_{y}{m:02}{d:02}"


def record_to_bq(date):
    target_dt = get_target_date(date)

    logger.info(f"target_dt: {target_dt}")

    y = target_dt.year
    m = target_dt.month
    d = target_dt.day

    csv_s3_prefix = os.path.join(
        os.environ['OUTPUT_CSV_PREFIX'], f"{y}/{m:02}/{d:02}")

    bq = BigQueryService(os.environ["GCP_PROJECT_ID"], os.environ["DATASET_NAME"])

    logger.info("delete existing partition and tables")
    for type, (result_table_name, log_table_name_prefix) in _TABLE_NAME_MAP.items():
        logger.info(f"delete [{type}] result table partition")
        bq.delete_partition(result_table_name, target_dt)
        logger.info(f"delete [{type}] today log table")
        bq.delete_table(os.environ["DATASET_NAME"], _log_table_name(log_table_name_prefix, y, m, d))

    for type, (result_table_name, log_table_name_prefix) in _TABLE_NAME_MAP.items():
        logger.info(f"agg [{type}] data")
        csv_dfs = [
            read_from_s3(os.environ["OUTPUT_CSV_BUCKET"], key) for key in load_file_list(
                os.environ["OUTPUT_CSV_BUCKET"], f"{csv_s3_prefix}/{type}")
        ]

        if len(csv_dfs) == 0:
            logger.warning(f"{type} csv does not exists.")
            continue

        csv_df = pd.concat(csv_dfs)
        csv_df["portfolio_id"] = csv_df["portfolio_id"].astype("Int64")

        bucket_key_prefix = os.path.join(
            os.environ["OUTPUT_CSV_PREFIX"],
            f"{y}/{m:02}/{d:02}/{y}{m:02}{d:02}_{os.environ['OUTPUT_STAGE']}_{type}",
        )

        logger.info(f"upload log [{type}] data")
        bucket_key_log = f"{bucket_key_prefix}_log.csv"
        write_df_to_s3(
            csv_df,
            os.environ["OUTPUT_CSV_BUCKET"], bucket_key_log)

        logger.info(f"upload result [{type}] data")
        bucket_key_result = f"{bucket_key_prefix}_result.csv"
        write_df_to_s3(
            format_to_bq_schema(csv_df, result_table_name),
            os.environ["OUTPUT_CSV_BUCKET"], bucket_key_result
        )

        logger.info(f"sync [{type}] result data to gcs")
        bq.s3_to_gcs(os.environ["OUTPUT_CSV_BUCKET"], os.environ["GCS_BUCKET"], bucket_key_result)

        logger.info(f"append [{type}] result data to bq")
        bq.append_gcs_to_bq(result_table_name, os.environ["GCS_BUCKET"], bucket_key_result)

        logger.info(f"create today [{type}] log data table to bq")
        bq.create_table_from_df(
            os.environ["DATASET_NAME"], _log_table_name(log_table_name_prefix, y, m, d), csv_df)


def lambda_handler(event, context):
    logger.info(f"start {event}")

    record_to_bq(**event)

    logger.info(f"finished {event}")
