import datetime
import pandas as pd
from typing import Callable

from spai.ai.boosting import Log1pModel, get_tuned_params
from .config import (
    FEATURE_COLUMNS,
    TARGET_COLUMN,
    CATEGORICAL_COLUMNS,
    OUTPUT_COLUMNS,
    THRESHOLD_OF_CLICKS_WEEKLY,
    THRESHOLD_OF_SALES_MONTHLY
)


class SPAModel(Log1pModel):
    pass


class SPAEstimator:

    def __init__(self,
                 model_writer: Callable[[object], None] = None,
                 model_reader: Callable[[None], object] = None,
                 output: Callable[[pd.DataFrame], None] = None,
                 is_tune: bool = True,
                 num_trials: int = 50):
        self._output = output
        self._model_writer = model_writer
        self._model_reader = model_reader
        self._is_tune = is_tune
        self._num_trials = num_trials

    def _cutoff(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[(df['ad_id_weekly_sum_clicks'] > THRESHOLD_OF_CLICKS_WEEKLY) &
                (df['ad_id_monthly_sum_sales'] > THRESHOLD_OF_SALES_MONTHLY)]
        return df

    def fit(self, df: pd.DataFrame):
        df_train = df[~df[TARGET_COLUMN].isnull()]
        df_train = self._cutoff(df)

        X_train = df_train[FEATURE_COLUMNS]
        y_train = df_train[TARGET_COLUMN]
        w_train = df_train["weight"]

        cat_features = [FEATURE_COLUMNS.index(col) for col in CATEGORICAL_COLUMNS]
        if self._is_tune:
            params = get_tuned_params(
                SPAModel,
                X_train,
                y_train,
                cat_features=cat_features,
                num_trials=self._num_trials,
                has_time=True,
                time=df_train['date'],
                weight=w_train,
            )
        else:
            # for debug/it to skip tuning
            params = {
                "params": {
                    "learning_rate": 0.01,
                    "objective": SPAModel.objective,
                    "metric": SPAModel.objective,
                    "verbose": -1,
                    "num_leaves": 15,
                },
                "num_boost_round": 30,
                "cat_features": cat_features,
            }

        # 学習
        model = SPAModel(params)
        model.fit(X_train, y_train, weight=w_train)

        if callable(self._model_writer):
            self._model_writer(model.to_bytes())
        else:
            self._model = model

    def predict(self, df: pd.DataFrame, today):
        yesterday = today - datetime.timedelta(days=1)
        if callable(self._model_reader):
            binary = self._model_reader()
            model = SPAModel.from_bytes(binary)
        else:
            model = self._model

        df[CATEGORICAL_COLUMNS] = (df[CATEGORICAL_COLUMNS].fillna(-1) + 1) \
            .astype(int)
        df_inference = df[df["date"] == yesterday].copy()

        X_inference = df_inference[FEATURE_COLUMNS]
        df_inference.loc[:, TARGET_COLUMN] = model.predict(X_inference)
        df_inference['date'] = today
        df_inference['portfolio_id'] = df_inference['portfolio_id'].astype("Int64")

        output = df_inference[OUTPUT_COLUMNS]

        # 予測結果を出力
        if callable(self._output):
            self._output(output)

        return output
