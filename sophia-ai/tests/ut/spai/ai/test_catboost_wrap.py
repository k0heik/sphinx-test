import pytest
import numpy as np
import pandas as pd
from spai.ai.boosting import CatBoostModel


@pytest.mark.parametrize("has_weight", [False, True])
@pytest.mark.parametrize(
    'params', [
        {},
        {
            'iterations': 100,
        },
        {
            'iterations': 100,
            'cat_features': [0],
        },
    ]
)
def test_catboost_model(params, has_weight):
    np.random.seed(42)
    model = CatBoostModel(params)

    X_cat = pd.DataFrame(
        np.random.randint(low=0, high=10, size=(100, 1)))
    X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
    X = pd.concat([X_cat, X_num], axis=1)

    y = pd.Series(np.random.randn(100))
    if has_weight:
        weight = pd.Series(np.random.rand(100))
    else:
        weight = None

    model.fit(X, y, weight)
    pred = model.predict(X)
    assert y.shape == pred.shape
    assert isinstance(pred, np.ndarray)

    # 重みが負でも学習できるか
    if has_weight:
        weight.iloc[0] = -1
        model.fit(X, y, weight)


def test_catboost_numpy_cat_error():
    np.random.seed(42)
    params = {
        'cat_features': [0]
    }
    model = CatBoostModel(params)

    X_cat = np.random.randint(low=0, high=10, size=(100, 1))
    X_num = np.random.randn(100, 9)
    X = np.concatenate([X_cat, X_num], axis=1)

    y = np.random.randn(100)
    # TypeErrorが上がれば良い
    try:
        model.fit(X, y)
    except TypeError:
        pass
    try:
        model.predict(X)
    except TypeError:
        pass

    # cat_featuresなしならエラーを吐かない
    model = CatBoostModel()
    model.fit(X, y)
    model.predict(X)


@pytest.fixture
def model():
    np.random.seed(42)
    params = {
        'cat_features': [0]
    }
    model = CatBoostModel(params)

    X_cat = pd.DataFrame(
        np.random.randint(low=0, high=10, size=(100, 1)))
    X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
    X = pd.concat([X_cat, X_num], axis=1)

    y = pd.Series(np.random.randn(100))
    model.fit(X, y, verbose=False)
    return model


@pytest.fixture
def X():
    X_cat = pd.DataFrame(np.random.randint(low=0, high=10, size=(100, 1)))
    X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
    X = pd.concat([X_cat, X_num], axis=1)
    return X


def test_io_bytes(model, X):
    model_bin = model.to_bytes()
    model_reconst = CatBoostModel.from_bytes(model_bin)

    pred = model.predict(X)
    pred_reconst = model_reconst.predict(X)

    np.testing.assert_array_equal(pred, pred_reconst)
