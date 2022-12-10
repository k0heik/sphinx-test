import tempfile
import catboost
from ..base import BaseMLModel
from typing import Union, List, Optional
import pandas as pd
import numpy as np
from ..utils import serialize, deserialize


class CatBoostModel(BaseMLModel):
    '''
    catboost regression model wrapper
    Attributes
    ----------
    params : dict
        dict of hyperparameters
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._model = catboost.CatBoostRegressor(**self.params)

    def fit(self, X: Union[np.ndarray, pd.DataFrame, List],
            y: Union[np.ndarray, pd.Series, List],
            weight: Optional[Union[np.ndarray, pd.Series, List]] = None,
            **kwargs):
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
        y = self._scale(y)
        self._model.fit(X=X, y=y, sample_weight=weight, **kwargs)

    def predict(self, X: Union[np.ndarray, pd.DataFrame, List],
                **kwargs) -> np.ndarray:
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

        data = {
            "model": model_bin,
            "params": self.params
        }
        return serialize(data)

    @classmethod
    def from_bytes(cls, binary: bytes) -> "CatBoostModel":
        """binaryからデシリアライズ
        Args:
            binary (bytes): モデルのバイナリ
        Returns:
            CatBoostModel: デシリアライズしたモデル
        """
        data = deserialize(binary)
        model_bin = data["model"]
        params = data["params"]

        self = CatBoostModel()
        self.params = params
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, "wb") as f:
                f.write(model_bin)
            self._model.load_model(tf.name)
        return self
