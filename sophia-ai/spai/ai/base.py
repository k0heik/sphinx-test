from abc import ABC, abstractmethod
import numpy as np


class BaseModel(ABC):
    '''
    MetaClass of model
    Attributes
    ----------
    params : dict
        dict of hyperparameters
    '''
    @abstractmethod
    def to_bytes(self):
        raise NotImplementedError

    @abstractmethod
    def from_bytes(binary: bytes):
        raise NotImplementedError


class BaseMLModel(BaseModel):
    '''
    MetaClass of ML model
    Attributes
    ----------
    params : dict
        dict of hyperparameters
    '''

    def __init__(self, params: dict = {}, **kwargs):
        self.params: dict = params
        self.params.update(kwargs)

    @abstractmethod
    def fit(self, X, y, weight=None):
        raise NotImplementedError

    @abstractmethod
    def predict(self, X):
        raise NotImplementedError

    @staticmethod
    def _clip_weight(weight):
        if weight is not None:
            weight = np.clip(weight, 0, None)
        return weight

    def _check_X_type_and_cat_features(self, X):
        '''
        check if X is np.ndarray and cat_features is used.
        '''
        if isinstance(self.params.get('cat_features', None), list):
            if isinstance(X, np.ndarray) and\
                    not np.issubdtype(X.dtype, np.integer):
                raise TypeError(
                    '''categorical features seem to be converted to float.
                    Please use pd.DataFrame instead.''')

    @staticmethod
    def _scale(y):
        return y

    @staticmethod
    def _inv_scale(y):
        return y
