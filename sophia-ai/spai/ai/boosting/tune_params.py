import warnings
import optuna
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import pandas as pd
from typing import Union, List, Optional, Dict

import catboost
import lightgbm as lgb
import category_encoders as ce
import time

from ..base import BaseModel
from .catboost_wrap import CatBoostModel
from .lgb_wrap import LGBModel


def _train_val_split(
    X, y,
    validation_ratio, validation_days,
    time, has_time,
    weight=None,
):
    if weight is None:
        weight = np.ones_like(y)
    X = pd.DataFrame(X)
    y = pd.Series(y)
    weight = pd.Series(weight)
    if has_time:
        time = np.array(time, dtype="datetime64[D]")
        lastest_day = np.max(time)

        shift_days = np.timedelta64(validation_days, "D")
        train_idx = time <= lastest_day - shift_days
        validation_idx = time > lastest_day - shift_days
    else:
        _, _, train_idx, validation_idx = train_test_split(
            X, np.arange(len(y)), test_size=validation_ratio
        )

    train_X = X.iloc[train_idx]
    validation_X = X.iloc[validation_idx]
    train_y = y.iloc[train_idx]
    validation_y = y.iloc[validation_idx]
    train_weight = weight.iloc[train_idx]
    validation_weight = weight.iloc[validation_idx]
    return (
        train_X, validation_X,
        train_y, validation_y,
        train_weight, validation_weight
    )


def get_tuned_params(
    ModelClass: BaseModel,
    X: Union[np.ndarray, pd.DataFrame, List],
    y: Union[np.ndarray, pd.Series, List],
    cat_features: Optional[List[int]] = None,
    validation_ratio: float = 0.3,
    validation_days: int = 7,
    num_trials: int = 100,
    has_time: bool = False,
    time: Optional[Union[np.ndarray, pd.Series, List]] = None,
    weight: Optional[Union[np.ndarray, pd.Series, List]] = None,
) -> Dict:
    """
    tune hyper parameters by optuna
    Parameters
    ----------
    ModelClass : BaseModel
        Class of the model that you want to train
    X : Union[np.ndarray, pd.DataFrame, List]
    y : Union[np.ndarray, pd.Series, List]
    cat_features : Optional[List[int]]
        list of index whose column is categorical feature
    validation_ratio : float, default 0.3
    validation_days : int, default 7
    num_trials : int, default 100
        number of trials to find best params
    has_time : bool, default False
    time : Optional[Union[np.ndarray, pd.Series, List]]
    weight : Optional[Union[np.ndarray, pd.Series, List]]
    Returns
    -------
    params : dict
        best hyper parameters
    """

    if weight is not None:
        if isinstance(weight, list):
            weight = np.clip(np.array(weight, dtype=np.float64), 0, None)
        elif isinstance(weight, pd.Series):
            weight = weight.clip(0, None)
        else:
            weight = np.clip(weight, 0, None)
    y = ModelClass._scale(y)
    if issubclass(ModelClass, CatBoostModel):
        return get_tuned_params_catboost(
            ModelClass,
            X,
            y,
            cat_features,
            validation_ratio,
            validation_days,
            num_trials,
            has_time,
            time,
            weight,
        )
    elif issubclass(ModelClass, LGBModel):
        return get_tuned_params_lgb(
            ModelClass,
            X,
            y,
            cat_features,
            validation_ratio,
            validation_days,
            num_trials,
            has_time,
            time,
            weight,
        )
    else:
        raise NotImplementedError


def get_tuned_params_catboost(
    ModelClass: CatBoostModel,
    X: Union[np.ndarray, pd.DataFrame, List],
    y: Union[np.ndarray, pd.Series, List],
    cat_features: Optional[List[int]] = None,
    validation_ratio: float = 0.3,
    validation_days: int = 7,
    num_trials: int = 100,
    has_time: bool = False,
    time: Optional[Union[np.ndarray, pd.Series, List]] = None,
    weight: Optional[Union[np.ndarray, pd.Series, List]] = None,
) -> Dict:
    trn_X, val_X, trn_y, val_y, trn_w, val_w = _train_val_split(
        X, y, validation_ratio, validation_days, time, has_time, weight
    )

    eval_set = catboost.Pool(
        val_X, val_y,
        cat_features=cat_features,
        weight=val_w
    )

    def objective(trial: optuna.trial.Trial) -> float:
        params = {
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 0.0, 100.0),
            "colsample_bylevel": trial.suggest_float(
                "colsample_bylevel", 0.01, 1.0
            ),
            "depth": trial.suggest_int("depth", 1, 12),
            "random_strength": trial.suggest_int("random_strength", 0, 100),
            "bagging_temperature": trial.suggest_loguniform(
                "bagging_temperature", 0.01, 100.0
            ),
            "cat_features": cat_features,
            "early_stopping_rounds": 100,
            "iterations": 9999,
            "verbose": 0,
        }

        model = ModelClass(params)
        model.fit(trn_X, trn_y, weight=trn_w, eval_set=eval_set)
        trial.set_user_attr("iterations", model._model.get_best_iteration())
        pred = model.predict(val_X)
        mse = mean_squared_error(pred, val_y, sample_weight=val_w)
        return mse

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=num_trials)

    trial = study.best_trial
    params = trial.params.copy()
    params["iterations"] = max(trial.user_attrs["iterations"], 1)
    if cat_features is not None:
        params["cat_features"] = cat_features
    params["has_time"] = has_time
    return params


def estimate_fitting_time(trn_X, trn_y, val_X, val_y, trn_w, val_w, params):
    d_trn = lgb.Dataset(trn_X, trn_y, weight=trn_w)
    d_val = lgb.Dataset(val_X, val_y, weight=val_w)
    params = params.copy()
    params["num_leaves"] = 255
    params["max_bin"] = 255
    s = time.time()
    lgb.train(
        params, d_trn, 9999, d_val,
        callbacks=[
            lgb.early_stopping(stopping_rounds=100, verbose=True),
            lgb.log_evaluation(100)
        ]
    )
    return time.time() - s


def get_tuned_params_lgb(
    ModelClass: LGBModel,
    X: Union[np.ndarray, pd.DataFrame, List],
    y: Union[np.ndarray, pd.Series, List],
    cat_features: Optional[List[int]] = None,
    validation_ratio: float = 0.3,
    validation_days: int = 7,
    num_trials: int = 100,
    has_time: bool = False,
    time: Optional[Union[np.ndarray, pd.Series, List]] = None,
    weight: Optional[Union[np.ndarray, pd.Series, List]] = None,
    lr: float = 0.01,
) -> Dict:
    params = {
        "objective": ModelClass.objective,
        "metric": ModelClass.objective,
        "learning_rate": lr,
        "boosting_type": "gbdt",
        "verbose": -1,
    }
    original_cat_features = cat_features
    if isinstance(X, pd.DataFrame):
        cat_features = [X.columns[i] for i in cat_features]
    trn_X, val_X, trn_y, val_y, trn_w, val_w = _train_val_split(
        X, y, validation_ratio, validation_days, time, has_time, weight
    )
    if cat_features:
        cbe = ce.CatBoostEncoder(cols=cat_features)
        trn_X = cbe.fit_transform(trn_X, trn_y)
        val_X = cbe.transform(val_X)

    fitting_time = estimate_fitting_time(
        trn_X, trn_y,
        val_X, val_y,
        trn_w, val_w,
        params
    )

    d_trn = lgb.Dataset(trn_X, trn_y, weight=trn_w)
    d_val = lgb.Dataset(val_X, val_y, weight=val_w)
    study = optuna.create_study(pruner=optuna.pruners.MedianPruner())
    tuner = optuna.integration.lightgbm.LightGBMTuner(
        params,
        train_set=d_trn,
        valid_sets=d_val,
        num_boost_round=9999,
        study=study,
        time_budget=fitting_time * num_trials,
        callbacks=[
            lgb.early_stopping(stopping_rounds=100, verbose=True),
            lgb.log_evaluation(100)
        ]
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        tuner.run()

    best_params = tuner.best_params
    best_booster = tuner.get_best_booster()
    params.update(best_params)
    num_boost_round = best_booster.best_iteration
    params = {"params": params, "num_boost_round": num_boost_round}
    if cat_features is not None:
        params["cat_features"] = original_cat_features

    return params
