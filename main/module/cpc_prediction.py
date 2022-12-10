import os
from spai.service.cpc.service import CPCPredictionService
from spai.service.cpc.preprocess import CPCPreprocessor
from spai.service.cpc.estimator import CPCEstimator
from common_module.datamanage_util import (
    read_s3_manager,
    read_labelencoder_manager,
    CPC_LATEST_LABEL_ENCODER_PREFIX,
    CPC_LATEST_MODEL_BIN_KEY,
)
from module import args


def exec(ad_df, campaign_placement_df):
    today = args.Event.today
    BUCKET = os.environ["BUCKET"]

    return CPCPredictionService(
        preprocessor=CPCPreprocessor(
            label_encoder_writer=None,
            label_encoder_reader=read_labelencoder_manager(
                BUCKET, CPC_LATEST_LABEL_ENCODER_PREFIX
            ),
            is_use_dask=False,
        ),
        estimator=CPCEstimator(
            model_reader=read_s3_manager(
                BUCKET,
                CPC_LATEST_MODEL_BIN_KEY,
            ),
            is_tune=False,
        ),
    ).predict(ad_df, campaign_placement_df, today)
