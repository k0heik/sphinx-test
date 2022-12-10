import os
from spai.service.cvr.service import CVRPredictionService
from spai.service.cvr.preprocess import CVRPreprocessor
from spai.service.cvr.estimator import CVREstimator
from common_module.datamanage_util import (
    read_s3_manager,
    read_labelencoder_manager,
    CVR_LATEST_LABEL_ENCODER_PREFIX,
    CVR_LATEST_MODEL_BIN_KEY,
)
from module import args


def exec(ad_df, keyword_queries_df):
    today = args.Event.today
    BUCKET = os.environ["BUCKET"]

    return CVRPredictionService(
        preprocessor=CVRPreprocessor(
            label_encoder_writer=None,
            label_encoder_reader=read_labelencoder_manager(
                BUCKET, CVR_LATEST_LABEL_ENCODER_PREFIX
            ),
            is_use_dask=False,
        ),
        estimator=CVREstimator(
            model_reader=read_s3_manager(
                BUCKET,
                CVR_LATEST_MODEL_BIN_KEY,
            ),
            is_tune=False,
        ),
    ).predict(ad_df, keyword_queries_df, today)
