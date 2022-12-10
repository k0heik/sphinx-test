import logging
import numpy as np
import pandas as pd
from typing import Callable

from spai.utils.kpi import get_C

from .config import FILLNA_COLUMNS


logger = logging.getLogger(__name__)


class BIDPreprocessor:
    def __init__(self,
                 output: Callable[[pd.DataFrame], None] = None):
        self._output = output

    def transform(self, df: pd.DataFrame, advertising_account_id: int = None, portfolio_id: int = None) -> pd.DataFrame:
        logger.info("start _preprocess")
        # 対象ユニットの抽出
        if advertising_account_id is not None:
            df = df[
                df["advertising_account_id"] ==
                advertising_account_id
            ]
            if portfolio_id is not None:
                df = df[
                    df["portfolio_id"] == portfolio_id
                ]

        logger.info(f"extract records: {len(df)}")
        if len(df) == 0:
            logger.warning("no extract data")
            return []

        df['date'] = pd.to_datetime(df['date'])

        df = self._fillna(df)

        df['C'] = df.apply(get_C, axis=1)

        logger.info("end _preprocess")

        if callable(self._output):
            self._output(df)

        return df

    def _fillna(self, df) -> pd.DataFrame:
        for column in df.columns:
            if column in FILLNA_COLUMNS:
                df[column] = df[column].fillna(0)

        # portfolio_id
        df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())

        # target_kpi_value は Nan の場合 None に置換
        df['target_kpi_value'] = df['target_kpi_value'].fillna(-1)
        df['target_kpi_value'] = np.where(df['target_kpi_value'] < 0,
                                          None,
                                          df['target_kpi_value'])

        # round_up_point
        df['round_up_point'] = df['round_up_point'].fillna(1).astype(int)

        return df
