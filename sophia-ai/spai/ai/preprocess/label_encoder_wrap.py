import numpy as np
from sklearn import preprocessing

from ..utils import serialize, deserialize
from ..base import BaseModel


class LabelEncoder(BaseModel):
    def __init__(self):
        self._le = preprocessing.LabelEncoder()

    def fit(self, data_list):
        """欠損値用にUnknownクラスを追加してからfitする
        """
        data_list = [str(x) for x in data_list]
        self._le = self._le.fit(list(data_list) + ["Unknown"])
        self.classes_ = self._le.classes_
        return self

    def transform(self, data_list):
        """欠損値はUnknownに変換してからtransformする
        """
        data_list = [str(x) for x in data_list]
        new_data_list = list(data_list)
        for unique_item in np.unique(data_list):
            if unique_item not in self._le.classes_:
                new_data_list = [
                    "Unknown" if x == unique_item else x for x in new_data_list
                ]

        return self._le.transform(new_data_list)

    def to_bytes(self) -> bytes:
        """binaryにシリアライズ
        Returns:
            bytes: モデルのバイナリ
        """
        return serialize(self._le)

    @classmethod
    def from_bytes(cls, binary: bytes) -> "LabelEncoder":
        """binaryからデシリアライズo
        Args:
            binary (bytes): モデルのバイナリ
        Returns:
            LabelEncoder: デシリアライズしたモデル
        """
        self = LabelEncoder()
        self._le = deserialize(binary)
        return self
