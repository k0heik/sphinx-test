import os
import pandas as pd
from common_module.aws_util import get_ssm_parameter, read_from_s3_uri
from common_module.logger_util import get_custom_logger


logger = get_custom_logger()


_TARGET_UNIT_DF_COLUMNS = [
    "advertising_account_id",
    "portfolio_id",
    "is_target_unit"
]


def get_target_unit_df():
    ssm_parameter_name = os.environ.get("SSM_TARGET_UNIT_PARAMETER_NAME", None)
    if ssm_parameter_name is None:
        raise ValueError("environment variable SSM_TARGET_UNIT_PARAMETER_NAME is not set.")

    try:
        target_unit_path = get_ssm_parameter(ssm_parameter_name)
        df = read_from_s3_uri(target_unit_path)

        df["portfolio_id"] = df["portfolio_id"].astype(pd.Int64Dtype())

        if set(df.columns) == set(_TARGET_UNIT_DF_COLUMNS):
            raise ValueError("SSM_TARGET_UNIT csv format is incorrect.")

        return df
    except Exception as e:
        raise ValueError(f"Error occured at load target_unit csv. [error detail] {str(e)}")
