import os
import time
import random
from common_module.logger_util import get_custom_logger
from common_module.aws_util import write_df_to_s3, write_binary_to_s3, read_binary_from_s3, copy_s3_file

logger = get_custom_logger()


CPC_LATEST_LABEL_ENCODER_PREFIX = "cpc/model/latest_le/"
CPC_LATEST_MODEL_BIN_KEY = "cpc/model/latest.bin"

CVR_LATEST_LABEL_ENCODER_PREFIX = "cvr/model/latest_le/"
CVR_LATEST_MODEL_BIN_KEY = "cvr/model/latest.bin"

SPA_LATEST_LABEL_ENCODER_PREFIX = "spa/model/latest_le/"
SPA_LATEST_MODEL_BIN_KEY = "spa/model/latest.bin"


def output_preprocess_to_s3_manager(s3_bucket, s3_key):
    def _output_preprocess_to_s3(df):
        logger.info("start write_df_to_s3")
        logger.info(f"s3_bucket: {s3_bucket}, s3_key: {s3_key}")
        write_df_to_s3(df, s3_bucket, s3_key)
        logger.info("finished write_df_to_s3")

    return _output_preprocess_to_s3


def output_preprocess_manager(s3_bucket, s3_key, bq, bq_dataset_name, bq_table_name):
    def _output_preprocess(df):
        logger.info("start write_df_to_s3")
        logger.info(f"s3_bucket: {s3_bucket}, s3_key: {s3_key}")
        write_df_to_s3(df, s3_bucket, s3_key)
        logger.info("finished write_df_to_s3")

        logger.info("start create_table_from_df")
        logger.info(f"bq_dataset_name: {bq_dataset_name}, bq_table_name: {bq_table_name}")
        bq.create_table_from_df(
            destination_dataset_name=bq_dataset_name,
            table_name=bq_table_name,
            df=df,
        )
        logger.info("finished create_table_from_df")

    return _output_preprocess


def output_distributed_preprocess_manager(
        s3_bucket,
        key,
        gcs_bucket,
        bq,
        bq_dataset_name,
        bq_table_name,
        max_trials=10,
        base_sleep_sec=1,
):
    def _output_distributed_preprocess(df):
        logger.info("start write_df_to_s3")
        logger.info(f"s3_bucket: {s3_bucket}, s3_key: {key}")
        write_df_to_s3(df, s3_bucket, key)
        logger.info("finished write_df_to_s3")

        logger.info("start to bq via gcs")
        logger.info(f"bq_dataset_name: {bq_dataset_name}, bq_table_name: {bq_table_name}")
        bq.s3_to_gcs(s3_bucket, gcs_bucket, key)
        for t in range(max_trials):
            try:
                bq.append_gcs_to_bq(bq_table_name, gcs_bucket, key, dataset_name=bq_dataset_name)
                return
            except Exception as e:
                logger.error(e)
                time.sleep(random.randint(10, base_sleep_sec * 2 ** t + 10))
        else:
            raise Exception(f"output_distributed_preprocess_manager failed: max_trials={max_trials}")

    return _output_distributed_preprocess


def write_latest_model_manager(s3_bucket, s3_key, s3_latest_key=None):
    def _write_latest_model(binary):
        logger.info("start write_binary_to_s3")
        logger.info(f"s3_bucket: {s3_bucket}, s3_key: {s3_key}")
        write_binary_to_s3(binary, s3_bucket, s3_key)
        logger.info("finished write_binary_to_s3")

        if s3_latest_key is not None:
            logger.info("start copy_s3_file ")
            logger.info(f"from: s3://{s3_bucket}/{s3_key}, to: s3://{s3_bucket}/{s3_key}")
            copy_s3_file(s3_bucket, s3_key, s3_bucket, s3_latest_key)
            logger.info("finished copy_s3_file")

    return _write_latest_model


def _get_labelencoder_name(s3_prefix, name):
    return f"{os.path.join(s3_prefix, name)}.bin"


def write_latest_labelencoder_manager(s3_bucket, s3_latest_prefix, s3_prefix):
    def _write_labelencoder(binary, name):
        for prefix in [s3_latest_prefix, s3_prefix]:
            s3_key = _get_labelencoder_name(prefix, name)
            logger.info("start write_binary_to_s3")
            logger.info(f"s3_bucket: {s3_bucket}, s3_key: {s3_key}")
            write_binary_to_s3(binary, s3_bucket, s3_key)
            logger.info("finished write_binary_to_s3")

    return _write_labelencoder


def read_labelencoder_manager(s3_bucket, s3_prefix):
    def _read_labelencoder(name):
        s3_key = _get_labelencoder_name(s3_prefix, name)
        logger.info("start read_binary_from_s3")
        logger.info(f"s3_bucket: {s3_bucket}, s3_key: {s3_key}")
        binary = read_binary_from_s3(s3_bucket, s3_key)
        logger.info("finished read_binary_from_s3")

        return binary

    return _read_labelencoder


def read_s3_manager(s3_bucket, s3_key):
    def _read_s3_manager():
        return read_binary_from_s3(s3_bucket, s3_key)

    return _read_s3_manager
