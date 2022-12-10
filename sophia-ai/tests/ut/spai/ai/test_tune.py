from spai.ai.base import BaseMLModel
import numpy as np
import pandas as pd
import pytest
from spai.ai.boosting import get_tuned_params, CatBoostModel, LGBModel, Log1pModel, ProbModel


@pytest.mark.parametrize(
    "ModelClass", [CatBoostModel, LGBModel, ProbModel, Log1pModel]
)
@pytest.mark.parametrize("has_time", [True, False])
@pytest.mark.parametrize("has_weight", [True, False])
def test_tune(ModelClass, has_time, has_weight):
    X_cat = pd.DataFrame(np.random.randint(low=0, high=10, size=(100, 1)))
    X_num = pd.DataFrame(np.random.randn(100, 9), columns=list(range(1, 10)))
    X = pd.concat([X_cat, X_num], axis=1)
    X.columns = ["col_" + str(col) for col in X.columns]
    y = pd.Series(np.random.randn(100))
    if has_weight:
        weight = pd.Series(np.random.rand(100))
    else:
        weight = None

    time = None
    if has_time:
        time = pd.date_range(start="1/1/2018", periods=len(X))

    params = get_tuned_params(
        ModelClass,
        X,
        y,
        num_trials=3,
        cat_features=[0],
        has_time=has_time,
        time=time,
        weight=weight,
    )
    model = ModelClass(params)

    model.fit(X, y, weight)
    pred = model.predict(X)
    assert pred.shape == y.shape

    if has_weight:
        # weightが負の場合
        weight.iloc[0] = -1
        model.fit(X, y, weight)
        pred = model.predict(X)
        assert pred.shape == y.shape

    # binary IO test
    binary = model.to_bytes()
    model_reconst = ModelClass.from_bytes(binary)
    pred_reconst = model_reconst.predict(X)
    np.testing.assert_array_equal(pred, pred_reconst)


def test_tune_invalid_modelclass():
    class ModelClass(BaseMLModel):
        pass

    X = [[1, 2]]
    y = [1]
    with pytest.raises(NotImplementedError):
        get_tuned_params(ModelClass, X, y)
