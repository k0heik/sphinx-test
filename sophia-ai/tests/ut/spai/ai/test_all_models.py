import pytest
import numpy as np
import pandas as pd
from spai.ai.boosting import Log1pModel, ProbModel


@pytest.mark.parametrize("has_weight", [True, False])
@pytest.mark.parametrize(
    'Model', [
        Log1pModel,
        ProbModel
    ]
)
def test_model(Model, has_weight):
    params = {'cat_features': [0], 'iterations': 10}
    model = Model(params)
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
    assert pred.shape == y.shape

    # validate output
    if isinstance(model, ProbModel):
        assert (0 <= pred).all()
        assert (pred <= 1).all()
    elif isinstance(model, Log1pModel):
        assert (0 <= pred).all()
