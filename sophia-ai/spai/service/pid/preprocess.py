import pandas as pd
from typing import Callable

from .config import FILLNA_COLUMNS, PREPROCESS_COLUMNS


class PIDPreprocessor:
    def __init__(self,
                 output: Callable[[pd.DataFrame], None] = None):
        self._output = output

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) > 0:
            df['portfolio_id'] = df['portfolio_id'].astype(pd.Int64Dtype())
            df['date'] = pd.to_datetime(df['date'])

            df = self._fillna(df)
        else:
            df = pd.DataFrame(columns=PREPROCESS_COLUMNS)

        if callable(self._output):
            self._output(df[PREPROCESS_COLUMNS])

        return df[PREPROCESS_COLUMNS]

    def _fillna(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in df.columns:
            if column in FILLNA_COLUMNS:
                df[column] = df[column].fillna(0)

        # not_ml_applied_daysがNULLの場合は、p,qの初期化が行われるようにNULL埋めする
        df['not_ml_applied_days'] = df['not_ml_applied_days'].fillna(9999)

        return df
