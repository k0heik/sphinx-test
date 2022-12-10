import os
from spai.service.spa.service import SPAPredictionService
from spai.service.spa.preprocess import SPAPreprocessor
from spai.service.spa.estimator import SPAEstimator
from common_module.datamanage_util import (
    read_s3_manager,
    read_labelencoder_manager,
    SPA_LATEST_LABEL_ENCODER_PREFIX,
    SPA_LATEST_MODEL_BIN_KEY,
)

from module import args


def exec(ad_df, keyword_queries_df):
    today = args.Event.today
    BUCKET = os.environ["BUCKET"]

    return SPAPredictionService(
        preprocessor=SPAPreprocessor(
            label_encoder_writer=None,
            label_encoder_reader=read_labelencoder_manager(
                BUCKET, SPA_LATEST_LABEL_ENCODER_PREFIX
            ),
            is_use_dask=False,
        ),
        estimator=SPAEstimator(
            model_reader=read_s3_manager(
                BUCKET,
                SPA_LATEST_MODEL_BIN_KEY,
            ),
            is_tune=False,
        ),
    ).predict(ad_df, keyword_queries_df, today)
