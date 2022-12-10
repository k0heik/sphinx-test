import tempfile
from typing import List, Union, Optional

import category_encoders as ce
import lightgbm as lgb
import numpy as np
import pandas as pd

from ..base import BaseMLModel
from ..utils import deserialize, serialize


class LGBModel(BaseMLModel):
    objective = "regression"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._model = None
        self._cbe = ce.CatBoostEncoder()

    @property
    def _cat_features(self):
        return self.params.get("cat_features", None)

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List],
        y: Union[np.ndarray, pd.Series, List],
        weight: Optional[Union[np.ndarray, pd.Series, List]] = None,
        **kwargs
    ):
        '''
        train this model w/ X and y
        Parameters
        ----------
        X : Union[np.ndarray, pd.DataFrame, List]
        y : Union[np.ndarray, pd.Series, List]
        weight : Optional[Union[np.ndarray, pd.Series, List]]
        '''
        weight = self._clip_weight(weight)
        self._check_X_type_and_cat_features(X)
        if self._cat_features:
            cat_features = [X.columns[i] for i in self._cat_features]
            self._cbe = ce.CatBoostEncoder(cols=cat_features)
            X = self._cbe.fit_transform(X, y)

        y = self._scale(y)
        d_train = lgb.Dataset(X, y, weight=weight)

        self._model = lgb.train(
            self.params.get("params", {"objective": self.objective}),
            train_set=d_train,
            num_boost_round=self.params.get("num_boost_round", 100),
        )

    def predict(
        self, X: Union[np.ndarray, pd.DataFrame, List], **kwargs
    ) -> np.ndarray:
        '''
        predict y w/ this trained model
        Parameters
        ----------
        X : Union[np.ndarray, pd.DataFrame, List]
        Returns
        -------
        pred : np.ndarray
            predicted values
        '''
        self._check_X_type_and_cat_features(X)
        if self._cat_features:
            X = self._cbe.transform(X)
        pred = self._model.predict(X, **kwargs)
        pred = self._inv_scale(pred)
        return pred

    def to_bytes(self) -> bytes:
        """binaryにシリアライズ
        Returns:
            bytes: モデルのバイナリ
        """
        with tempfile.NamedTemporaryFile() as tf:
            self._model.save_model(tf.name)
            with open(tf.name, "rb") as f:
                model_bin = f.read()

        data = {"model": model_bin, "cbe": self._cbe, "params": self.params}
        return serialize(data)

    @classmethod
    def from_bytes(cls, binary: bytes) -> "LGBModel":
        """binaryからデシリアライズ
        Args:
            binary (bytes): モデルのバイナリ
        Returns:
            LGBModel: デシリアライズしたモデル
        """
        data = deserialize(binary)
        model_bin = data["model"]
        cbe = data["cbe"]
        params = data["params"]

        self = cls()
        self.params = params
        self._cbe = cbe
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, "wb") as f:
                f.write(model_bin)
            self._model = lgb.Booster(model_file=tf.name)
        return self


class ProbModel(LGBModel):
    objective = "xentropy"

    @staticmethod
    def _scale(y):
        # clip targets unbounded in [0, 1] by delayed conversions
        return np.clip(y, 0, 1)


class Log1pModel(LGBModel):
    objective = "regression"

    @staticmethod
    def _scale(y):
        # ignore negatives
        y = np.clip(y, 0, None)
        y = np.log1p(y)
        return y

    @staticmethod
    def _inv_scale(y):
        y = np.expm1(y)
        # clip in [0, oo]
        y = np.clip(y, 0, None)
        return y
