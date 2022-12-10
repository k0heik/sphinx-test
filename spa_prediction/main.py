import os
import sys
from click import argument, group, pass_context
import datetime
from dateutil import parser
from pytz import timezone
import pandas as pd

from spai.service.spa.service import SPAPredictionService
from spai.service.spa.preprocess import SPAPreprocessor
from spai.service.spa.estimator import SPAEstimator

from common_module.aws_util import write_df_to_s3
from common_module.bigquery_util import BigQueryService
from common_module.exception_util import business_exception
from common_module.logger_util import get_custom_logger
from common_module.system_util import get_s3_key
from common_module.datamanage_util import (
    output_preprocess_to_s3_manager,
    write_latest_model_manager,
    write_latest_labelencoder_manager,
)


logger = get_custom_logger()

NUM_TRIALS = int(os.getenv("NUM_TRIALS_FOR_TUNING", 50))
USE_TUNING = os.getenv("USE_TUNING", "true").lower() == "true"
DASK_BATCH_SIZE = int(os.getenv("DASK_BATCH_SIZE", 8))


@group(invoke_without_command=True)
@pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        print(ctx.get_help())


@cli.command(
    help='train spa prediction model',
    name='train')
@argument('date')
def train(date: str):
    tz = timezone(os.getenv('TZ', "Asia/Tokyo"))
    today = parser.parse(date).astimezone(tz) \
        if date != "latest" else datetime.datetime.now(tz)
    logger.info(f"date: {today}")

    BUCKET = os.environ['BUCKET']
    GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
    DATASET_NAME = os.environ['DATASET_NAME']
    COMMERCE_FLOW_DATASET_NAME = os.environ['COMMERCE_FLOW_DATASET_NAME']

    # データ抽出
    logger.info("start extract")

    bq = BigQueryService(GCP_PROJECT_ID, DATASET_NAME)
    params = {
        "project": GCP_PROJECT_ID,
        "dataset": DATASET_NAME,
        "commerce_flow_dataset": COMMERCE_FLOW_DATASET_NAME,
        "today": today.strftime("%Y-%m-%d"),
        "num_days_ago": 365 * 2 + 30 * 2,
    }
    df = bq.extract_to_df(
        "./sql",
        "extract.tpl.sql",
        params,
    )
    df_ad = bq.extract_to_df(
        "./sql",
        "extract_ad_info.tpl.sql",
        params,
    )
    df_keyword_queries = bq.extract_to_df(
        "./sql",
        "extract_keyword_queries.tpl.sql",
        params,
    )
    logger.info("finished extract")

    logger.info("start extracted to s3")
    write_df_to_s3(
        df,
        BUCKET,
        get_s3_key("spa/extract/", today, "train_data.csv")
    )
    write_df_to_s3(
        df_ad,
        BUCKET,
        get_s3_key("spa/extract/", today, "train_ad_data.csv")
    )
    write_df_to_s3(
        df_keyword_queries,
        BUCKET,
        get_s3_key("spa/extract/", today, "train_data_keyword_queries.csv")
    )
    logger.info("finished extracted to s3")

    df = pd.merge(df, df_ad, how='left', on=[
        "uid",
        "advertising_account_id",
        "portfolio_id",
        "campaign_id",
        "ad_group_id",
        "ad_type",
        "ad_id",
    ])

    if len(df) == 0:
        business_exception('Data does not exist.', logger)

    # SPA train 前処理＆実行
    logger.info("start fit")
    SPAPredictionService(
        preprocessor=SPAPreprocessor(
            label_encoder_writer=write_latest_labelencoder_manager(
                BUCKET,
                "spa/model/latest_le/",
                f"spa/preprocess/{today.year}/{today.month:02}/{today.day:02}/le/"
            ),
            label_encoder_reader=None,
            output=output_preprocess_to_s3_manager(
                BUCKET, f"spa/preprocess/{today.year}/{today.month:02}/{today.day:02}/train_data.csv",
            ),
            is_use_dask=True,
            batch_size=DASK_BATCH_SIZE,
        ),
        estimator=SPAEstimator(
            model_writer=write_latest_model_manager(
                BUCKET,
                get_s3_key("spa/model/", today, "model.bin"),
                "spa/model/latest.bin",
            ),
            is_tune=True,
        ),
    ).train(df, df_keyword_queries)
    logger.info("finished fit")

    logger.info("complete training pipeline")


if __name__ == '__main__':
    cli()

    # A zero exit code causes the job to be marked a Succeeded.
    sys.exit(0)
