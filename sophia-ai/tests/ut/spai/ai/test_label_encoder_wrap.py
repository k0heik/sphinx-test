import numpy as np
import pytest

from spai.ai.preprocess import LabelEncoder


@pytest.fixture
def model():
    np.random.seed(42)
    X = np.random.randint(low=10, high=100, size=100)
    model = LabelEncoder()
    model.fit(X)
    return model


@pytest.fixture
def X():
    np.random.seed(100)
    X = np.random.randint(low=10, high=100, size=100)
    return X


def test_io_bytes(model, X):
    model_bin = model.to_bytes()
    model_reconst = LabelEncoder.from_bytes(model_bin)

    label = model.transform(X)
    label_reconst = model_reconst.transform(X)
    np.testing.assert_array_equal(label, label_reconst)
